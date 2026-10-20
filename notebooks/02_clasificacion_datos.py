# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Data Classification: encontrar y etiquetar datos sensibles
# MAGIC
# MAGIC **Objetivo.** Descubrir qué columnas del esquema `banco` contienen datos personales y dejar una etiqueta `class.*` en cada una, para que las políticas de acceso se puedan escribir sobre la etiqueta y no sobre el nombre de la columna.
# MAGIC
# MAGIC Este notebook tiene dos caminos:
# MAGIC
# MAGIC | Camino | Cuándo | Qué hace |
# MAGIC |---|---|---|
# MAGIC | **A. Data Classification nativo** | Su workspace lo tiene disponible | Activa el escaneo sobre el catálogo; Databricks escribe las etiquetas `class.*` solo |
# MAGIC | **B. Escáner del taller** | Su workspace no lo tiene (Free Edition suele mostrar *"Data Classification is not available in this workspace"*) | Hace lo mismo con expresiones regulares, una regla personalizada para la cédula y `ai_classify` para el texto libre |
# MAGIC
# MAGIC Las dos rutas terminan en el mismo lugar: etiquetas `class.*` en `information_schema.column_tags`. Todo lo que sigue (máscaras, políticas, auditoría) funciona igual con las dos.
# MAGIC
# MAGIC **Antes de empezar:** ejecuten `01_setup_taller`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo")
dbutils.widgets.text("esquema", "banco", "Esquema")
CAT = dbutils.widgets.get("catalogo")
SCH = dbutils.widgets.get("esquema")
FQ = f"{CAT}.{SCH}"

# COMMAND ----------

# MAGIC %md ## 1. Camino A: ¿está disponible Data Classification en este workspace?

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
RUTA_CFG = f"/api/data-classification/v1/catalogs/{CAT}/config"


def consultar_config():
    try:
        return w.api_client.do("GET", RUTA_CFG)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def activar_nativo():
    """Intenta crear la configuración de Data Classification del catálogo."""
    try:
        w.api_client.do("POST", f"/api/data-classification/v1/catalogs/{CAT}/config", body={})
        return True, "Configuración creada"
    except Exception as e:  # noqa: BLE001
        return False, str(e)


ok, mensaje = activar_nativo()
NATIVO = ok or "already exists" in mensaje.lower()
print("Data Classification nativo:", "DISPONIBLE" if NATIVO else "NO DISPONIBLE en este workspace")
print("Detalle:", mensaje[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC Si salió **DISPONIBLE**, abran **Catalog > taller_gobierno > Details > Data classification** y confirmen que el escaneo está activo. Los primeros resultados tardan unos minutos. Revisen las etiquetas con la consulta del paso 4 y **omitan el camino B**.
# MAGIC
# MAGIC Si salió **NO DISPONIBLE**, continúen con el camino B. Es el mismo ejercicio hecho a mano, y deja claro **qué hace por debajo** la clasificación automática.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camino B: el escáner
# MAGIC
# MAGIC Tomamos una muestra de cada columna de texto y la comparamos contra reglas. Una columna se etiqueta cuando **al menos el 80%** de su muestra cumple la regla; así toleramos los valores sucios que sembramos a propósito.
# MAGIC
# MAGIC | Etiqueta | Regla |
# MAGIC |---|---|
# MAGIC | `class.email_address` | patrón de correo |
# MAGIC | `class.phone_number` | patrón `+código número` |
# MAGIC | `class.credit_card` | 16 dígitos que empiezan por 4 o 5 |
# MAGIC | `class.iban_code` | dos letras y 20 dígitos |
# MAGIC | `class.date_of_birth` | columna de tipo fecha cuyo nombre sugiere nacimiento |
# MAGIC | `class.name` | `ai_classify` sobre una muestra de valores |
# MAGIC | `pii_cedula_identidad` | **regla personalizada**: 9 dígitos en una columna cuyo nombre sugiere identificación |
# MAGIC | `pii_texto_libre` | **regla personalizada**: `ai_classify` detecta datos personales dentro de la frase |

# COMMAND ----------

import re

N_MUESTRA = 300
UMBRAL = 0.80

PATRONES = {
    "class.email_address": re.compile(r"^[\w.+-]+@[\w-]+\.[\w.-]+$"),
    "class.phone_number": re.compile(r"^\+\d{3} \d{4} \d{4}$"),
    "class.credit_card": re.compile(r"^[45]\d{15}$"),
    "class.iban_code": re.compile(r"^[A-Z]{2}\d{20}$"),
}
# Clasificador personalizado: valor + pista en el nombre de la columna
CEDULA = re.compile(r"^\d{9}$")
PISTA_CEDULA = re.compile(r"cedula|identific|dni|documento", re.I)
PISTA_NACIMIENTO = re.compile(r"nacimiento|birth", re.I)

ETIQUETAS_IA = [
    "nombre_de_persona",
    "texto_libre_con_datos_personales",
    "nombre_de_comercio",
    "categoria_o_codigo",
]


def columnas_de_texto():
    filas = spark.sql(
        f"""SELECT table_name, column_name, data_type
            FROM {CAT}.information_schema.columns
            WHERE table_schema = '{SCH}' AND data_type IN ('STRING', 'DATE')
            ORDER BY table_name, ordinal_position"""
    ).collect()
    return [(r.table_name, r.column_name, r.data_type) for r in filas]


def muestra(tabla, columna):
    filas = spark.sql(
        f"SELECT `{columna}` AS v FROM {FQ}.{tabla} WHERE `{columna}` IS NOT NULL LIMIT {N_MUESTRA}"
    ).collect()
    return [str(r.v) for r in filas]


def clasificar_con_ia(valores):
    texto = " | ".join(valores[:12])[:1500]
    df = spark.createDataFrame([(texto,)], ["t"])
    etiquetas = ", ".join(f"'{e}'" for e in ETIQUETAS_IA)
    return df.selectExpr(f"ai_classify(t, array({etiquetas})) AS c").collect()[0].c


resultados = []
for tabla, columna, tipo in columnas_de_texto():
    valores = muestra(tabla, columna)
    if not valores:
        continue
    etiqueta, motivo = None, ""

    if tipo == "DATE":
        if PISTA_NACIMIENTO.search(columna):
            etiqueta, motivo = "class.date_of_birth", "tipo fecha + nombre de columna"
    else:
        for tag, patron in PATRONES.items():
            if sum(bool(patron.match(v)) for v in valores) / len(valores) >= UMBRAL:
                etiqueta, motivo = tag, "patrón"
                break
        if etiqueta is None and PISTA_CEDULA.search(columna):
            if sum(bool(CEDULA.match(v)) for v in valores) / len(valores) >= UMBRAL:
                etiqueta, motivo = "pii_cedula_identidad", "regla personalizada"
        if etiqueta is None and not columna.startswith("id_"):
            ia = clasificar_con_ia(valores)
            if ia == "nombre_de_persona":
                etiqueta, motivo = "class.name", "ai_classify"
            elif ia == "texto_libre_con_datos_personales":
                etiqueta, motivo = "pii_texto_libre", "ai_classify (regla personalizada)"

    if etiqueta:
        resultados.append((tabla, columna, etiqueta, motivo))

import pandas as pd

df_res = pd.DataFrame(resultados, columns=["tabla", "columna", "etiqueta", "motivo"])
display(df_res)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escribir las etiquetas
# MAGIC
# MAGIC Una etiqueta de Unity Catalog es un par clave y valor sobre la columna. Las de `class.*` no llevan valor. Unity Catalog reserva el prefijo `class.` para las clases del sistema, por eso nuestras reglas personalizadas usan el prefijo `pii_`.

# COMMAND ----------

for tabla, columna, etiqueta, _ in resultados:
    spark.sql(f"ALTER TABLE {FQ}.{tabla} ALTER COLUMN `{columna}` SET TAGS ('{etiqueta}' = '')")
print(f"{len(resultados)} columnas etiquetadas")

# COMMAND ----------

# MAGIC %md ## 4. Revisar el resultado
# MAGIC
# MAGIC Esta consulta funciona igual si las etiquetas las escribió Databricks (camino A) o el escáner (camino B).

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, column_name, tag_name
FROM {CAT}.information_schema.column_tags
WHERE schema_name = '{SCH}' AND (tag_name LIKE 'class.%' OR tag_name LIKE 'pii\\_%')
ORDER BY table_name, column_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cobertura: ¿qué columnas de texto quedaron sin clasificar?
# MAGIC
# MAGIC Una columna sin etiqueta no es necesariamente segura: puede ser un dato no personal o un falso negativo. Revísenla con el equipo de gobierno.

# COMMAND ----------

display(spark.sql(f"""
SELECT c.table_name, c.column_name, c.data_type
FROM {CAT}.information_schema.columns c
LEFT JOIN {CAT}.information_schema.column_tags t
  ON t.schema_name = c.table_schema AND t.table_name = c.table_name
 AND t.column_name = c.column_name AND (t.tag_name LIKE 'class.%' OR t.tag_name LIKE 'pii\\_%')
WHERE c.table_schema = '{SCH}' AND c.data_type IN ('STRING', 'DATE') AND t.tag_name IS NULL
ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Proteger lo que encontramos: máscara de columna
# MAGIC
# MAGIC Con las etiquetas listas, aplicamos una máscara a **todas** las columnas con `class.*`, sin escribir una regla por columna.
# MAGIC
# MAGIC **Se hace sobre copias** (`*_protegida`) y no sobre las tablas originales: la máscara también aplica al motor de DQX Studio y a los monitores, que correrían sobre datos enmascarados y reportarían calidad falsa. Es una lección que aparece en cuanto se activan máscaras sobre tablas que otras herramientas también leen.
# MAGIC
# MAGIC La función deja ver el valor completo solo a los miembros del grupo `auditoria_pii`.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.seguridad COMMENT 'Funciones de máscara y filtro'")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CAT}.seguridad.enmascarar(valor STRING)
RETURNS STRING
COMMENT 'Muestra el valor solo a miembros de auditoria_pii; el resto ve asteriscos y los últimos 4 caracteres'
RETURN CASE
  WHEN is_member('auditoria_pii') THEN valor
  WHEN valor IS NULL THEN NULL
  ELSE concat(repeat('*', greatest(length(valor) - 4, 0)), right(valor, 4))
END
""")

tablas_con_pii = sorted({t for t, _, _, _ in resultados})
for tabla in tablas_con_pii:
    spark.sql(f"CREATE OR REPLACE TABLE {FQ}.{tabla}_protegida AS SELECT * FROM {FQ}.{tabla}")
    for t, columna, etiqueta, _ in resultados:
        if t == tabla and etiqueta != "class.date_of_birth":
            spark.sql(
                f"ALTER TABLE {FQ}.{tabla}_protegida ALTER COLUMN `{columna}` "
                f"SET MASK {CAT}.seguridad.enmascarar"
            )
print("Copias protegidas:", [f"{t}_protegida" for t in tablas_con_pii])

# COMMAND ----------

# MAGIC %md ### Antes: ustedes todavía **no** son miembros de `auditoria_pii`

# COMMAND ----------

display(spark.sql(f"SELECT id_cliente, nombre, cedula, correo, telefono FROM {FQ}.clientes_protegida LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Después: crean el grupo y se agregan
# MAGIC
# MAGIC Esto es lo que haría el equipo de gobierno al aprobar una excepción de auditoría. En Free Edition el grupo se crea en el workspace.

# COMMAND ----------

from databricks.sdk.service.iam import ComplexValue, Patch, PatchOp, PatchSchema

yo = w.current_user.me()
grupos = [g for g in w.groups.list(filter="displayName eq 'auditoria_pii'")]
grupo = grupos[0] if grupos else w.groups.create(display_name="auditoria_pii")
w.groups.patch(
    id=grupo.id,
    operations=[Patch(op=PatchOp.ADD, path="members", value=[{"value": yo.id}])],
    schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
)
print("Agregado al grupo auditoria_pii:", yo.user_name)

# COMMAND ----------

import time

time.sleep(15)  # la membresía tarda unos segundos en reflejarse
display(spark.sql(f"SELECT id_cliente, nombre, cedula, correo, telefono FROM {FQ}.clientes_protegida LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC Si todavía ven asteriscos, esperen un minuto y vuelvan a ejecutar la celda anterior: la membresía de grupos se cachea.
# MAGIC
# MAGIC ### Limpieza opcional
# MAGIC Para quitarse del grupo y repetir la demostración, ejecuten:
# MAGIC
# MAGIC ```python
# MAGIC w.groups.patch(id=grupo.id, operations=[Patch(op=PatchOp.REMOVE, path=f'members[value eq "{yo.id}"]')],
# MAGIC                schemas=[PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])
# MAGIC ```
