# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Preparar el taller: un banco regional sintético
# MAGIC
# MAGIC Este notebook crea, en el workspace de ustedes, todo lo que necesitan para el taller:
# MAGIC
# MAGIC | Objeto | Filas | Para qué lo usamos |
# MAGIC |---|---|---|
# MAGIC | `clientes` | ~5.000 | Datos personales (nombre, cédula, correo, teléfono, fecha de nacimiento) y errores de calidad sembrados |
# MAGIC | `cuentas` | 7.500 | Número de cuenta y saldos |
# MAGIC | `transacciones` | 60.000 | Número de tarjeta, montos y canales |
# MAGIC | `prestamos` | 3.000 | Cartera y mora, tabla del monitor de perfilamiento |
# MAGIC | `bitacora_contactos` | 2.000 | Texto libre con datos personales dentro de la nota |
# MAGIC
# MAGIC Todos los datos son **100% sintéticos y deterministas**: si dos personas ejecutan el notebook, obtienen exactamente los mismos números.
# MAGIC
# MAGIC **Cómputo:** serverless. **Tiempo:** 2 a 3 minutos.
# MAGIC
# MAGIC Los errores de calidad están sembrados a propósito, con porcentajes conocidos. Los vamos a encontrar tres veces con tres herramientas distintas: el perfilador de DQX, el monitor de Databricks y las reglas que ustedes mismos crean en DQX Studio.

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo")
dbutils.widgets.text("esquema", "banco", "Esquema")

CAT = dbutils.widgets.get("catalogo")
SCH = dbutils.widgets.get("esquema")
FQ = f"{CAT}.{SCH}"
print("Destino:", FQ)

# COMMAND ----------

# MAGIC %md ## 1. Catálogo y esquema

# COMMAND ----------

spark.sql(f"""CREATE CATALOG IF NOT EXISTS {CAT}
              COMMENT 'Taller de gobierno y calidad de datos. Datos 100% sintéticos'""")
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {FQ}
              COMMENT 'Banco regional sintético: clientes, cuentas, transacciones, préstamos y bitácora de contactos'""")
spark.sql(f"USE CATALOG {CAT}")
spark.sql(f"USE SCHEMA {SCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clientes
# MAGIC
# MAGIC Errores sembrados (porcentajes sobre las ~5.000 filas):
# MAGIC
# MAGIC | Error | Cantidad aproximada |
# MAGIC |---|---|
# MAGIC | `cedula` nula o con el texto `N/A` | 3% |
# MAGIC | `correo` sin arroba | 1,5% |
# MAGIC | `pais` con código inválido `XX` | 1% |
# MAGIC | `ingreso_mensual` negativo | 1% |
# MAGIC | `fecha_alta` en el futuro | 0,5% |
# MAGIC | `id_cliente` repetido | 30 filas |

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.clientes
COMMENT 'Clientes del banco. Contiene datos personales sintéticos'
AS
WITH base AS (
  SELECT id,
         CASE WHEN id > 4970 THEN id - 4970 ELSE id END AS id_dup,
         pmod(hash(id, 1), 1000) AS r_nom,
         pmod(hash(id, 2), 1000) AS r_ape
  FROM range(1, 5001)
),
cat AS (
  SELECT *,
    array('Andrea','Luis','María','Carlos','Sofía','Diego','Valeria','Jorge','Camila','Pablo','Daniela','Mateo',
          'Lucía','Andrés','Paula','Sebastián','Fernanda','Ricardo','Gabriela','Esteban') AS nombres,
    array('Rodríguez','Hernández','López','Martínez','González','Pérez','Sánchez','Ramírez','Torres','Flores',
          'Vargas','Jiménez','Castro','Morales','Rojas','Mora','Cruz','Díaz','Reyes','Ortiz') AS apellidos,
    array('CR','CR','CR','GT','GT','HN','NI','SV','PA','PA') AS paises,
    array('masivo','masivo','masivo','preferente','preferente','pyme') AS segmentos
  FROM base
)
SELECT
  concat('C', lpad(CAST(id_dup AS STRING), 6, '0'))                                   AS id_cliente,
  nombres[pmod(hash(id, 3), size(nombres))]                                           AS nombre,
  apellidos[pmod(hash(id, 4), size(apellidos))]                                       AS apellido,
  CASE WHEN pmod(hash(id, 5), 100) < 2 THEN NULL
       WHEN pmod(hash(id, 5), 100) = 2 THEN 'N/A'
       ELSE CAST(100000000 + pmod(hash(id, 6), 800000000) AS STRING) END              AS cedula,
  CASE WHEN pmod(hash(id, 7), 1000) < 15
       THEN concat(lower(nombres[pmod(hash(id, 3), size(nombres))]), id, 'correo.com')
       ELSE concat(lower(translate(nombres[pmod(hash(id, 3), size(nombres))], 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU')), '.',
                   lower(translate(apellidos[pmod(hash(id, 4), size(apellidos))], 'áéíóúÁÉÍÓÚ', 'aeiouAEIOU')),
                   id, '@correo-sintetico.com') END                                   AS correo,
  concat('+', CASE paises[pmod(hash(id, 8), size(paises))]
                WHEN 'CR' THEN '506' WHEN 'GT' THEN '502' WHEN 'HN' THEN '504'
                WHEN 'NI' THEN '505' WHEN 'SV' THEN '503' ELSE '507' END,
         ' ', CAST(6000 + pmod(hash(id, 9), 3999) AS STRING), ' ',
         lpad(CAST(pmod(hash(id, 10), 10000) AS STRING), 4, '0'))                     AS telefono,
  CASE WHEN pmod(hash(id, 11), 100) = 0 THEN 'XX'
       ELSE paises[pmod(hash(id, 8), size(paises))] END                               AS pais,
  date_add(DATE'1952-01-01', pmod(hash(id, 12), 17000))                               AS fecha_nacimiento,
  segmentos[pmod(hash(id, 13), size(segmentos))]                                      AS segmento,
  CASE WHEN pmod(hash(id, 14), 100) = 0 THEN -1 * (200 + pmod(hash(id, 15), 800))
       ELSE CAST(400 + pmod(hash(id, 15), 9600) AS DOUBLE) END                        AS ingreso_mensual,
  CASE WHEN pmod(hash(id, 16), 200) = 0 THEN date_add(current_date(), 30 + pmod(hash(id, 17), 300))
       ELSE date_add(DATE'2015-01-01', pmod(hash(id, 17), 4000)) END                  AS fecha_alta
FROM cat
""")
spark.sql(f"SELECT count(*) AS filas FROM {FQ}.clientes").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cuentas
# MAGIC
# MAGIC Errores sembrados: saldos negativos en cuentas de ahorro (0,5%) y cuentas sin `id_cliente` válido (1%).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.cuentas
COMMENT 'Cuentas de depósito de los clientes'
AS
WITH base AS (SELECT id FROM range(1, 7501)),
cat AS (
  SELECT id,
    array('ahorro','corriente','nomina') AS tipos,
    array('USD','USD','USD','CRC','GTQ','HNL','NIO','PAB') AS monedas,
    array('activa','activa','activa','activa','inactiva','bloqueada') AS estados
  FROM base
)
SELECT
  concat('A', lpad(CAST(id AS STRING), 7, '0'))                                     AS id_cuenta,
  CASE WHEN pmod(hash(id, 21), 100) = 0 THEN concat('C9', lpad(CAST(id AS STRING), 5, '0'))
       ELSE concat('C', lpad(CAST(1 + pmod(hash(id, 22), 4970) AS STRING), 6, '0')) END AS id_cliente,
  concat('CR', lpad(CAST(pmod(hash(id, 23), 100) AS STRING), 2, '0'), '0152',
         lpad(CAST(pmod(hash(id, 24), 1000000000) AS STRING), 9, '0'),
         lpad(CAST(pmod(hash(id, 25), 100000) AS STRING), 5, '0'))                  AS numero_cuenta,
  tipos[pmod(hash(id, 26), size(tipos))]                                            AS tipo,
  monedas[pmod(hash(id, 27), size(monedas))]                                        AS moneda,
  CASE WHEN pmod(hash(id, 28), 200) = 0 AND tipos[pmod(hash(id, 26), size(tipos))] = 'ahorro'
       THEN -1 * round(pmod(hash(id, 29), 50000) / 100.0, 2)
       ELSE round(pmod(hash(id, 29), 2500000) / 100.0, 2) END                      AS saldo,
  date_add(DATE'2012-01-01', pmod(hash(id, 30), 5000))                              AS fecha_apertura,
  estados[pmod(hash(id, 31), size(estados))]                                        AS estado
FROM cat
""")
spark.sql(f"SELECT count(*) AS filas FROM {FQ}.cuentas").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transacciones
# MAGIC
# MAGIC Las fechas se generan hacia atrás desde hoy, para que la tabla se vea fresca el día del taller. Errores sembrados: montos en cero o negativos (0,3%) y tarjetas con longitud incorrecta (0,5%).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.transacciones
COMMENT 'Movimientos de cuentas y tarjetas. Contiene número de tarjeta sintético'
AS
WITH base AS (SELECT id FROM range(1, 60001)),
cat AS (
  SELECT id,
    array('app','web','cajero','pos','sucursal') AS canales,
    array('Supermercado Central','Gasolinera Norte','Farmacia Vida','Tienda Online','Restaurante Sol',
          'Aerolínea Regional','Cine Plaza','Ferretería Unión','Servicio Eléctrico','Transporte Urbano') AS comercios,
    array('CR','CR','CR','GT','HN','NI','SV','PA') AS paises
  FROM base
)
SELECT
  concat('T', lpad(CAST(id AS STRING), 8, '0'))                                     AS id_tx,
  concat('A', lpad(CAST(1 + pmod(hash(id, 41), 7500) AS STRING), 7, '0'))           AS id_cuenta,
  timestampadd(SECOND, pmod(hash(id, 42), 86400),
               CAST(date_sub(current_date(), pmod(hash(id, 43), 90)) AS TIMESTAMP)) AS fecha_tx,
  CASE WHEN pmod(hash(id, 44), 1000) < 3 THEN 0.0
       WHEN pmod(hash(id, 44), 1000) < 6 THEN -1 * round(pmod(hash(id, 45), 20000) / 100.0, 2)
       ELSE round(5 + pmod(hash(id, 45), 500000) / 100.0, 2) END                   AS monto,
  'USD'                                                                             AS moneda,
  canales[pmod(hash(id, 46), size(canales))]                                        AS canal,
  comercios[pmod(hash(id, 47), size(comercios))]                                    AS comercio,
  paises[pmod(hash(id, 48), size(paises))]                                          AS pais_tx,
  CASE WHEN pmod(hash(id, 49), 200) = 0
       THEN concat('4', lpad(CAST(pmod(hash(id, 50), 1000000) AS STRING), 6, '0'))
       ELSE concat('4', lpad(CAST(pmod(hash(id, 50), 1000000) AS STRING), 6, '0'),
                   lpad(CAST(pmod(hash(id, 51), 1000000000) AS STRING), 9, '0')) END AS numero_tarjeta
FROM cat
""")
spark.sql(f"SELECT count(*) AS filas, min(fecha_tx) AS desde, max(fecha_tx) AS hasta FROM {FQ}.transacciones").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Préstamos
# MAGIC
# MAGIC Es la tabla del monitor de perfilamiento. Se genera como un **corte mensual** de los últimos 6 meses para que el monitor tenga una serie de tiempo. Errores sembrados: `saldo_capital` mayor al monto desembolsado (1%), tasa en cero (1%) y `pais` nulo (1%). Además, un incidente de negocio: en el último corte, el préstamo 1 de cada 12 pasa de 90 días de mora, contra 1 de cada 100 en los cortes anteriores.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.prestamos
COMMENT 'Cartera de préstamos, un registro por préstamo y corte mensual'
AS
WITH prest AS (SELECT id FROM range(1, 501)),
cortes AS (SELECT explode(sequence(0, 5)) AS k),
base AS (SELECT p.id, c.k FROM prest p CROSS JOIN cortes c),
cat AS (
  SELECT id, k,
    array('hipotecario','vehiculo','consumo','pyme','consumo','vehiculo') AS productos,
    array('CR','CR','GT','HN','NI','SV','PA','PA') AS paises
  FROM base
)
SELECT
  concat('P', lpad(CAST(id AS STRING), 6, '0'))                                     AS id_prestamo,
  concat('C', lpad(CAST(1 + pmod(hash(id, 61), 4970) AS STRING), 6, '0'))           AS id_cliente,
  productos[pmod(hash(id, 62), size(productos))]                                    AS producto,
  CASE WHEN pmod(hash(id, 63), 100) = 0 THEN NULL
       ELSE paises[pmod(hash(id, 64), size(paises))] END                            AS pais,
  CAST(2000 + pmod(hash(id, 65), 98000) AS DOUBLE)                                  AS monto_desembolsado,
  CASE WHEN pmod(hash(id, 66), 100) = 0
       THEN CAST(2000 + pmod(hash(id, 65), 98000) AS DOUBLE) * 1.2
       ELSE round(CAST(2000 + pmod(hash(id, 65), 98000) AS DOUBLE) * (0.35 + pmod(hash(id, 67), 60) / 100.0)
                  * (1 - k * 0.02), 2) END                                          AS saldo_capital,
  CASE WHEN pmod(hash(id, 68), 100) = 0 THEN 0.0
       ELSE round(6 + pmod(hash(id, 69), 1400) / 100.0, 2) END                      AS tasa_interes,
  -- Línea base: 1 de cada 100 préstamos con mora mayor a 90 días en los cortes anteriores.
  -- Incidente: en el último corte (k = 0) la mora mayor a 90 días sube al préstamo 1 de cada 12.
  CASE WHEN k = 0 AND pmod(hash(id, 70), 12) = 0 THEN 95 + pmod(hash(id, 71), 60)
       WHEN k > 0 AND pmod(hash(id, 74), 100) = 0 THEN 95 + pmod(hash(id, 71), 60)
       WHEN pmod(hash(id, 70), 12) = 1 THEN 35 + pmod(hash(id, 72), 40)
       ELSE 0 END                                                                   AS dias_mora,
  date_add(DATE'2019-01-01', pmod(hash(id, 73), 2500))                              AS fecha_desembolso,
  last_day(add_months(current_date(), -k))                                          AS fecha_corte
FROM cat
""")
spark.sql(f"SELECT count(*) AS filas, count(DISTINCT fecha_corte) AS cortes FROM {FQ}.prestamos").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bitácora de contactos
# MAGIC
# MAGIC Una columna de texto libre, `nota`, con datos personales **dentro** de la frase. Es el caso donde un clasificador por patrón de columna no alcanza y se necesita un clasificador personalizado o un modelo.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.bitacora_contactos
COMMENT 'Registro de contactos del centro de servicio. El texto libre contiene datos personales'
AS
WITH c AS (
  SELECT
    id,
    concat('C', lpad(CAST(1 + pmod(hash(id, 81), 4970) AS STRING), 6, '0')) AS id_cliente
  FROM range(1, 2001)
)
SELECT
  concat('B', lpad(CAST(c.id AS STRING), 6, '0'))                       AS id_contacto,
  c.id_cliente,
  array('llamada','chat','sucursal','correo')[pmod(hash(c.id, 82), 4)]  AS canal,
  date_sub(current_date(), pmod(hash(c.id, 83), 60))                    AS fecha,
  CASE pmod(hash(c.id, 84), 4)
    WHEN 0 THEN concat('El cliente ', cl.nombre, ' ', cl.apellido, ' pide actualizar su teléfono a ', cl.telefono, '.')
    WHEN 1 THEN concat('Se confirma el correo ', cl.correo, ' para el envío del estado de cuenta de ', cl.nombre, '.')
    WHEN 2 THEN concat(cl.nombre, ' ', cl.apellido, ' reporta un cargo desconocido en la tarjeta que termina en ',
                       lpad(CAST(pmod(hash(c.id, 85), 10000) AS STRING), 4, '0'), '.')
    ELSE 'Consulta general sobre horarios de sucursal, sin datos del cliente.' END AS nota
FROM c
JOIN (SELECT id_cliente, nombre, apellido, correo, telefono
      FROM (SELECT *, row_number() OVER (PARTITION BY id_cliente ORDER BY nombre, apellido, telefono) AS rn
            FROM {FQ}.clientes) WHERE rn = 1) cl
  ON cl.id_cliente = c.id_cliente
""")
spark.sql(f"SELECT count(*) AS filas FROM {FQ}.bitacora_contactos").show()

# COMMAND ----------

# MAGIC %md ## 7. Verificación
# MAGIC
# MAGIC Estas cifras deben coincidir con las de las demás personas del taller.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'clientes' AS tabla, count(*) AS filas FROM {FQ}.clientes
UNION ALL SELECT 'cuentas', count(*) FROM {FQ}.cuentas
UNION ALL SELECT 'transacciones', count(*) FROM {FQ}.transacciones
UNION ALL SELECT 'prestamos', count(*) FROM {FQ}.prestamos
UNION ALL SELECT 'bitacora_contactos', count(*) FROM {FQ}.bitacora_contactos
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
  count(*)                                                       AS filas,
  count(DISTINCT id_cliente)                                     AS clientes_unicos,
  sum(CASE WHEN cedula IS NULL OR cedula = 'N/A' THEN 1 END)     AS cedula_invalida,
  sum(CASE WHEN correo NOT LIKE '%@%' THEN 1 END)                AS correo_sin_arroba,
  sum(CASE WHEN pais = 'XX' THEN 1 END)                          AS pais_invalido,
  sum(CASE WHEN ingreso_mensual < 0 THEN 1 END)                  AS ingreso_negativo,
  sum(CASE WHEN fecha_alta > current_date() THEN 1 END)          AS alta_en_el_futuro
FROM {FQ}.clientes
"""))
