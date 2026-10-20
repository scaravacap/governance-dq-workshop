# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Alimentar la tabla, y romperla a propósito
# MAGIC
# MAGIC Anomaly detection aprende el ritmo normal de cada tabla del esquema (cada cuánto se actualiza y cuántas filas recibe) y avisa cuando algo se sale de ese patrón. Para verlo funcionar necesitan dos cosas: una tabla que reciba datos con regularidad y un día en que deje de hacerlo.
# MAGIC
# MAGIC Este notebook agrega un lote de transacciones nuevas a `transacciones`. Tiene tres modos:
# MAGIC
# MAGIC | Modo | Qué hace | Qué debería detectar el monitor |
# MAGIC |---|---|---|
# MAGIC | `normal` | Agrega unas 500 filas con fecha de hoy | Nada, es el ritmo habitual |
# MAGIC | `volumen_bajo` | Agrega solo 5 filas | Completitud: llegó mucho menos de lo esperado |
# MAGIC | `pausa` | No agrega nada | Frescura: la tabla dejó de actualizarse |
# MAGIC
# MAGIC **Cómo usarlo en el taller.** Programen este notebook como job cada hora en modo `normal` (el paso 3 les muestra cómo). Dejen que corra el tiempo que tengan, cambien el job a `pausa` y esperen. En un día de taller no van a ver la alerta de frescura, porque el modelo necesita varios días de historia. Lo que sí ven en minutos es la tabla de resultados del esquema y el estado de cada tabla.
# MAGIC
# MAGIC **Cómputo:** serverless. **Tiempo:** menos de 1 minuto por corrida.

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo")
dbutils.widgets.text("esquema", "banco", "Esquema")
dbutils.widgets.dropdown("modo", "normal", ["normal", "volumen_bajo", "pausa"], "Modo")

CAT = dbutils.widgets.get("catalogo")
SCH = dbutils.widgets.get("esquema")
MODO = dbutils.widgets.get("modo")
FQ = f"{CAT}.{SCH}"
print("Destino:", FQ, "| modo:", MODO)

# COMMAND ----------

# MAGIC %md ## 1. Agregar el lote

# COMMAND ----------

filas = {"normal": 500, "volumen_bajo": 5, "pausa": 0}[MODO]

if filas == 0:
    print("Modo pausa: no se agrega nada. La tabla queda quieta y el monitor lo va a notar.")
else:
    spark.sql(f"""
    INSERT INTO {FQ}.transacciones
    WITH base AS (SELECT id FROM range(1, {filas} + 1)),
    ult AS (SELECT max(CAST(substr(id_tx, 2) AS BIGINT)) AS m FROM {FQ}.transacciones),
    cat AS (
      SELECT id, ult.m + id AS n,
        array('app','web','cajero','pos','sucursal') AS canales,
        array('Supermercado Central','Gasolinera Norte','Farmacia Vida','Tienda Online','Restaurante Sol',
              'Aerolínea Regional','Cine Plaza','Ferretería Unión','Servicio Eléctrico','Transporte Urbano') AS comercios,
        array('CR','CR','CR','GT','HN','NI','SV','PA') AS paises
      FROM base CROSS JOIN ult
    )
    SELECT
      concat('T', lpad(CAST(n AS STRING), 8, '0'))                              AS id_tx,
      concat('A', lpad(CAST(1 + pmod(hash(n, 41), 7500) AS STRING), 7, '0'))    AS id_cuenta,
      current_timestamp()                                                       AS fecha_tx,
      round(5 + pmod(hash(n, 45), 500000) / 100.0, 2)                           AS monto,
      'USD'                                                                     AS moneda,
      canales[pmod(hash(n, 46), size(canales))]                                 AS canal,
      comercios[pmod(hash(n, 47), size(comercios))]                             AS comercio,
      paises[pmod(hash(n, 48), size(paises))]                                   AS pais_tx,
      concat('4', lpad(CAST(pmod(hash(n, 50), 1000000) AS STRING), 6, '0'),
             lpad(CAST(pmod(hash(n, 51), 1000000000) AS STRING), 9, '0'))      AS numero_tarjeta
    FROM cat
    """)
    print(f"Se agregaron {filas} filas a {FQ}.transacciones")

spark.sql(f"SELECT count(*) AS filas, max(fecha_tx) AS ultima_tx FROM {FQ}.transacciones").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ver lo que ve el monitor
# MAGIC
# MAGIC Abran **Catalog > taller_gobierno > banco > Quality**. Ahí aparece el estado de cada tabla del esquema. Las columnas que importan son la última actualización y el volumen de filas.
# MAGIC
# MAGIC El historial de commits de la tabla es la misma señal que usa el monitor, así que pueden verla desde SQL:

# COMMAND ----------

display(spark.sql(f"""
SELECT version, timestamp, operation, operationMetrics['numOutputRows'] AS filas_escritas
FROM (DESCRIBE HISTORY {FQ}.transacciones)
ORDER BY version DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Programarlo como job cada hora
# MAGIC
# MAGIC 1. En el menú izquierdo, **Jobs & Pipelines > Create > Job**.
# MAGIC 2. Tarea de tipo **Notebook**, apunten a este archivo, cómputo **Serverless**.
# MAGIC 3. En **Job parameters** agreguen `modo` = `normal`.
# MAGIC 4. **Add trigger > Scheduled**, cada hora.
# MAGIC
# MAGIC Cuando quieran provocar el incidente, edite el parámetro a `pausa` o `volumen_bajo`. En Free Edition tienen un límite de jobs concurrentes, así que dejen este job como el único programado.
