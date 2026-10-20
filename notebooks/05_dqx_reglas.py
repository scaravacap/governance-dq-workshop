# Databricks notebook source
# MAGIC %md
# MAGIC # 05. DQX: calidad en tránsito, con código
# MAGIC
# MAGIC **DQX** es la librería open source de Databricks Labs para validar DataFrames y tablas. Aplican reglas **fila por fila** y separan lo bueno de lo malo **antes** de escribir el destino.
# MAGIC
# MAGIC En este notebook:
# MAGIC
# MAGIC 1. **Perfilan** `clientes` y DQX les propone reglas candidatas.
# MAGIC 2. **Escriben sus propias reglas** como YAML, las aplican y separan válidos de cuarentena.
# MAGIC 3. **Cuentan** cuántos registros cayó cada regla y lo comparan con los errores que sembramos en `01_setup_taller`.
# MAGIC 4. **Guardan** las reglas en una tabla, que es el puente hacia el siguiente ejercicio: crear reglas **sin código** en DQX Studio.
# MAGIC
# MAGIC **Antes de empezar:** ejecuten `01_setup_taller`. **Cómputo:** serverless.

# COMMAND ----------

# MAGIC %pip install -q databricks-labs-dqx==0.16.0
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo")
dbutils.widgets.text("esquema", "banco", "Esquema")
CAT = dbutils.widgets.get("catalogo")
SCH = dbutils.widgets.get("esquema")
FQ = f"{CAT}.{SCH}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.calidad COMMENT 'Reglas DQX, cuarentenas y métricas de calidad'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Perfilar: DQX propone reglas desde los datos
# MAGIC
# MAGIC El perfilador mira nulos, rangos y valores frecuentes, y los traduce a reglas candidatas. Son un **punto de partida**: aprenden también de los errores que ya trae la tabla.

# COMMAND ----------

import pandas as pd
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
df = spark.table(f"{FQ}.clientes")

resumen, perfiles = DQProfiler(ws).profile(df, options={"sample_fraction": 1.0, "limit": 10_000})

display(pd.DataFrame(
    [{"columna": p.column, "perfil": p.name, "descripcion": p.description, "parametros": str(p.parameters)} for p in perfiles]
))

# COMMAND ----------

propuestas = DQGenerator(ws).generate_dq_rules(perfiles, criticality="warn")
print(yaml.safe_dump(propuestas, allow_unicode=True, sort_keys=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Qué observar.**
# MAGIC
# MAGIC - Propone `is_not_null` donde casi no hay nulos, y rangos de fecha y de ingreso **calculados con los datos sucios**: el rango de `ingreso_mensual` incluye los negativos que sembramos.
# MAGIC - No propone nada sobre el **formato** de la cédula ni del correo: eso lo saben las personas del negocio, no los datos.
# MAGIC
# MAGIC Por eso el flujo sano es: el perfilador propone, gobierno corrige y aprueba.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Escribir las reglas del negocio
# MAGIC
# MAGIC Cada regla tiene un nombre, una **criticidad** (`error` mueve la fila a cuarentena, `warn` solo la anota) y una función con sus argumentos.
# MAGIC
# MAGIC Un detalle que cuesta una tarde la primera vez: en `is_in_list`, los valores de texto van **con comillas simples dentro de las comillas del YAML** (`"'CR'"`). DQX los interpreta como expresiones SQL, y `CR` sin comillas se lee como una columna.

# COMMAND ----------

reglas = yaml.safe_load("""
- name: id_cliente_presente
  criticality: error
  check:
    function: is_not_null
    arguments: {column: id_cliente}
- name: id_cliente_unico
  criticality: error
  check:
    function: is_unique
    arguments: {columns: [id_cliente]}
- name: cedula_presente
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments: {column: cedula}
- name: cedula_formato_numerico
  criticality: error
  check:
    function: regex_match
    arguments: {column: cedula, regex: '^[0-9]{9}$'}
- name: correo_valido
  criticality: warn
  check:
    function: regex_match
    arguments: {column: correo, regex: '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'}
- name: pais_operacion_del_banco
  criticality: error
  check:
    function: is_in_list
    arguments: {column: pais, allowed: ["'CR'", "'GT'", "'HN'", "'NI'", "'SV'", "'PA'"]}
- name: ingreso_positivo
  criticality: warn
  check:
    function: sql_expression
    arguments:
      expression: ingreso_mensual > 0
      msg: El ingreso mensual debe ser mayor a cero
- name: fecha_alta_no_futura
  criticality: error
  check:
    function: is_not_in_future
    arguments: {column: fecha_alta}
""")
print(f"{len(reglas)} reglas")

# COMMAND ----------

# MAGIC %md ## 3. Aplicar y separar
# MAGIC
# MAGIC `apply_checks_by_metadata_and_split` devuelve dos DataFrames: `validos` con los registros sin ningún `error` (los que solo tienen `warn` pasan), y `cuarentena` con los que tienen al menos un `error` **o** un `warn`, con las columnas `_errors` y `_warnings` que dicen **qué regla falló**. Por eso un registro con solo una advertencia aparece en los dos.

# COMMAND ----------

dq = DQEngine(ws)
validos, cuarentena = dq.apply_checks_by_metadata_and_split(spark.table(f"{FQ}.clientes"), reglas)

n_total = spark.table(f"{FQ}.clientes").count()
print(f"Total: {n_total} | válidos: {validos.count()} | cuarentena: {cuarentena.count()}  (esperen ~4.690 y ~439; las advertencias suman a los dos lados)")

# COMMAND ----------

display(cuarentena.select("id_cliente", "cedula", "correo", "pais", "ingreso_mensual", "fecha_alta", "_errors", "_warnings").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Cuántas filas atrapó cada regla?
# MAGIC
# MAGIC Se cuenta sobre **toda** la tabla con las reglas como anotaciones, sin separar. Compárenlo con la tabla de errores sembrados de `01_setup_taller`.

# COMMAND ----------

from pyspark.sql import functions as F

anotado = dq.apply_checks_by_metadata(spark.table(f"{FQ}.clientes"), reglas)
errores = anotado.select(F.explode_outer("_errors").alias("e")).select("e.name", "e.message")
avisos = anotado.select(F.explode_outer("_warnings").alias("e")).select("e.name", "e.message")
display(
    errores.where("name IS NOT NULL").groupBy("name").count().withColumn("criticidad", F.lit("error"))
    .unionByName(avisos.where("name IS NOT NULL").groupBy("name").count().withColumn("criticidad", F.lit("warn")))
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Esperen estas cifras (la cédula se reparte en dos reglas: nulas contra el texto `N/A`):
# MAGIC
# MAGIC | Error sembrado | Filas | Regla que lo atrapa |
# MAGIC |---|---|---|
# MAGIC | cédula nula o `N/A` | 169 | `cedula_presente` (113 nulas) y `cedula_formato_numerico` (56 con `N/A`) |
# MAGIC | correo sin arroba | 92 | `correo_valido` |
# MAGIC | país `XX` | 52 | `pais_operacion_del_banco` |
# MAGIC | ingreso negativo | 53 | `ingreso_positivo` |
# MAGIC | fecha de alta futura | 29 | `fecha_alta_no_futura` |
# MAGIC | `id_cliente` repetido | 60 filas | `id_cliente_unico` |
# MAGIC
# MAGIC Una advertencia sobre `is_in_list`: **deja pasar los nulos**. Si `pais` fuera nulo, esa regla no lo detectaría. Para cubrirlo se necesita además `is_not_null_and_not_empty` sobre `pais`. Es un hueco clásico: millones en saldos sin país pueden llegar a una tabla de oro con todas las reglas en verde.

# COMMAND ----------

# MAGIC %md ## 4. Guardar: válidos, cuarentena y reglas

# COMMAND ----------

from databricks.labs.dqx.config import TableChecksStorageConfig

validos.drop("_errors", "_warnings").write.mode("overwrite").saveAsTable(f"{FQ}.clientes_validos")
cuarentena.write.mode("overwrite").saveAsTable(f"{FQ}.clientes_cuarentena")
spark.sql(f"COMMENT ON TABLE {FQ}.clientes_cuarentena IS 'Clientes que violaron al menos una regla DQX, con el detalle en _errors y _warnings'")

dq.save_checks(
    reglas,
    TableChecksStorageConfig(location=f"{CAT}.calidad.dqx_checks", run_config_name="clientes", mode="overwrite"),
)
display(spark.table(f"{CAT}.calidad.dqx_checks"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para llevarse
# MAGIC
# MAGIC - Las reglas viven en una **tabla**, no en el código del pipeline: gobierno las puede revisar y versionar.
# MAGIC - El mismo conjunto de reglas se puede aplicar en un job, en un pipeline declarativo (como expectations) o desde **DQX Studio**.
# MAGIC - Siguiente ejercicio: **DQX Studio**. Las mismas reglas, creadas por una persona de negocio sin escribir YAML.
