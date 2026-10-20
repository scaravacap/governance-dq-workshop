# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Data Quality Monitoring: calidad en reposo
# MAGIC
# MAGIC Dos capacidades de Databricks que miran las tablas **después** de que los datos ya aterrizaron:
# MAGIC
# MAGIC | Capacidad | Nivel | Qué detecta | Cómo se configura |
# MAGIC |---|---|---|---|
# MAGIC | **Anomaly detection** | Esquema completo | Tablas que dejaron de actualizarse (frescura) y caídas de volumen (completitud) | Un solo interruptor por esquema, sin reglas |
# MAGIC | **Data Profiling** | Una tabla | Estadísticas por columna, deriva entre cortes y **métricas de negocio personalizadas** | Un monitor con una serie de tiempo y sus métricas |
# MAGIC
# MAGIC En este notebook activamos las dos desde código. Después las ven en la interfaz, en **Catalog > Quality**.
# MAGIC
# MAGIC **Antes de empezar:** ejecuten `01_setup_taller`. **Cómputo:** serverless.

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo")
dbutils.widgets.text("esquema", "banco", "Esquema")
CAT = dbutils.widgets.get("catalogo")
SCH = dbutils.widgets.get("esquema")
FQ = f"{CAT}.{SCH}"

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.calidad COMMENT 'Salida de los monitores de calidad'")


def id_esquema(nombre_completo):
    return w.schemas.get(nombre_completo).schema_id


def id_tabla(nombre_completo):
    return w.tables.get(nombre_completo).table_id


MONITORES = "/api/data-quality/v1/monitors"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anomaly detection sobre todo el esquema `banco`
# MAGIC
# MAGIC Un monitor de esquema aprende el ritmo normal de actualización y el volumen de cada tabla. Si una tabla se atrasa o recibe mucho menos de lo habitual, lo marca. No hay reglas que escribir, y por eso es la primera línea de defensa.

# COMMAND ----------

sid = id_esquema(FQ)
try:
    w.api_client.do(
        "POST", MONITORES,
        body={"object_type": "schema", "object_id": sid, "anomaly_detection_config": {}},
    )
    print("Anomaly detection activado sobre", FQ)
except Exception as e:  # noqa: BLE001
    if "already" in str(e).lower() or "exists" in str(e).lower():
        print("Ya estaba activado sobre", FQ)
    else:
        raise

print(w.api_client.do("GET", f"{MONITORES}/schema/{sid}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Qué esperar.** El modelo necesita historia para saber qué es normal: los primeros resultados llegan después del primer escaneo y se vuelven útiles conforme pasan los días. Por eso el taller trae un notebook aparte, `04_alimentar_y_romper`, que mantiene viva una tabla y permite provocar un incidente para que lo vean en pantalla.
# MAGIC
# MAGIC Dónde mirar: **Catalog > taller_gobierno > banco > Quality**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Profiling sobre `prestamos`, con métricas de negocio
# MAGIC
# MAGIC El monitor calcula estadísticas por columna **por corte mensual** y por segmentos (`pais`, `producto`). Además le agregamos cinco métricas propias:
# MAGIC
# MAGIC | Métrica | Tipo | Qué dice |
# MAGIC |---|---|---|
# MAGIC | `saldo_total` | agregada | Suma del saldo de capital |
# MAGIC | `saldo_vencido_90` | agregada | Saldo con más de 90 días de mora |
# MAGIC | `pct_sin_pais` | agregada | Porcentaje de préstamos sin país |
# MAGIC | `indice_mora_90` | derivada | `saldo_vencido_90 / saldo_total` |
# MAGIC | `delta_indice_mora_90` | deriva | Cambio del índice contra el corte anterior |

# COMMAND ----------

import json

bodega = next((x for x in w.warehouses.list()), None)
assert bodega, "Necesitan un SQL warehouse (en Free Edition existe el Serverless Starter Warehouse)"
assets_dir = f"/Workspace/Users/{me}/taller_gobierno/data_profiling"
w.workspace.mkdirs(assets_dir)


def metrica(nombre, tipo, definicion, tipo_dato="double"):
    return {
        "name": nombre,
        "type": f"DATA_PROFILING_CUSTOM_METRIC_TYPE_{tipo}",
        "input_columns": [":table"],
        "definition": definicion,
        "output_data_type": json.dumps({"name": nombre, "type": tipo_dato, "nullable": True, "metadata": {}}),
    }


cuerpo = {
    "object_type": "table",
    "object_id": id_tabla(f"{FQ}.prestamos"),
    "data_profiling_config": {
        "output_schema_id": id_esquema(f"{CAT}.calidad"),
        "assets_dir": assets_dir,
        "warehouse_id": bodega.id,
        "time_series": {
            "timestamp_column": "fecha_corte",
            "granularities": ["AGGREGATION_GRANULARITY_1_MONTH"],
        },
        "slicing_exprs": ["pais", "producto"],
        "custom_metrics": [
            metrica("saldo_total", "AGGREGATE", "SUM(saldo_capital)"),
            metrica("saldo_vencido_90", "AGGREGATE", "SUM(CASE WHEN dias_mora > 90 THEN saldo_capital ELSE 0 END)"),
            metrica("pct_sin_pais", "AGGREGATE", "AVG(CASE WHEN pais IS NULL THEN 1.0 ELSE 0.0 END)"),
            # Las métricas derivadas y de deriva citan a las otras por nombre, sin llaves
            metrica("indice_mora_90", "DERIVED", "saldo_vencido_90 / NULLIF(saldo_total, 0)"),
            metrica("delta_indice_mora_90", "DRIFT", "{{current_df}}.indice_mora_90 - {{base_df}}.indice_mora_90"),
        ],
    },
}
try:
    w.api_client.do("POST", MONITORES, body=cuerpo)
    print("Monitor de perfilamiento creado sobre prestamos")
except Exception as e:  # noqa: BLE001
    if "already" in str(e).lower() or "exists" in str(e).lower():
        print("Ya existía el monitor sobre prestamos")
    else:
        raise

# COMMAND ----------

# MAGIC %md ### Lanzar la primera corrida y esperar el resultado

# COMMAND ----------

import time

tid = id_tabla(f"{FQ}.prestamos")
# Al crear el monitor, Databricks lanza solo la primera corrida. Un refresh manual
# mientras esa corrida está PENDING se rechaza, así que aquí solo esperamos.
FINALES = ("MONITOR_REFRESH_STATE_SUCCESS", "MONITOR_REFRESH_STATE_FAILED", "MONITOR_REFRESH_STATE_CANCELED")
for _ in range(120):
    corridas = w.api_client.do("GET", f"{MONITORES}/table/{tid}/refreshes").get("refreshes", [])
    estado = corridas[0].get("state") if corridas else "sin corridas todavía"
    print(" ", estado)
    if estado in FINALES:
        break
    time.sleep(15)
# La primera corrida tarda entre 10 y 15 minutos en Free Edition. Mientras esperan,
# sigan con el notebook 05 (reglas DQX) y vuelvan aquí.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Las métricas que acaba de generar
# MAGIC
# MAGIC El monitor deja dos tablas en `calidad`: `prestamos_profile_metrics` y `prestamos_drift_metrics`. Aquí la serie del índice de mora por corte.
# MAGIC
# MAGIC Fíjense en el salto del índice, de menos del 1% a cerca del 9%: es el incidente de negocio que sembramos en `01_setup_taller`. En nuestras pruebas la primera corrida produjo solo las ventanas de los dos cortes más recientes, y por eso el incidente está entre esos dos. Ninguna regla de calidad lo detecta, porque cada fila es válida. Solo se ve en la serie.

# COMMAND ----------

display(spark.sql(f"""
SELECT window.start AS corte, indice_mora_90, saldo_total, saldo_vencido_90, pct_sin_pais
FROM {CAT}.calidad.prestamos_profile_metrics
WHERE column_name = ':table' AND slice_key IS NULL AND log_type = 'INPUT'
ORDER BY corte
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT window.start AS corte, delta_indice_mora_90
FROM {CAT}.calidad.prestamos_drift_metrics
WHERE column_name = ':table' AND slice_key IS NULL AND drift_type = 'CONSECUTIVE'
ORDER BY corte
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por país
# MAGIC El mismo índice, segmentado con el `slicing_exprs`. En el corte actual, ¿en qué país se concentra la mora?

# COMMAND ----------

display(spark.sql(f"""
SELECT slice_value AS pais, round(indice_mora_90, 4) AS indice_mora_90, round(saldo_total) AS saldo_total
FROM {CAT}.calidad.prestamos_profile_metrics
WHERE column_name = ':table' AND slice_key = 'pais' AND log_type = 'INPUT'
  AND window.start = (SELECT max(window.start) FROM {CAT}.calidad.prestamos_profile_metrics)
ORDER BY indice_mora_90 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Para explorar en la interfaz
# MAGIC
# MAGIC 1. **Catalog > taller_gobierno > banco > prestamos > Quality**: abran el tablero que Databricks generó junto con el monitor.
# MAGIC 2. Cambien a la pestaña de deriva y comparen el corte actual contra el anterior.
# MAGIC 3. En **Catalog > taller_gobierno > banco > Quality**, revisen el estado de anomaly detection por tabla.
