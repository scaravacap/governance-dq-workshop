# Taller de gobierno y calidad de datos

Un taller práctico, de unas cuatro horas, para encontrar datos sensibles, vigilar la calidad de las tablas y crear reglas de calidad sin código. Todo corre en **Databricks Free Edition**, con datos 100% sintéticos de un banco regional.

Los tres temas del taller:

| Tema | Qué hacen ustedes | Herramienta |
|---|---|---|
| **Data Classification** | Encuentran columnas con datos personales, las etiquetan con `class.*` y les aplican una máscara con una sola función | Etiquetas de Unity Catalog, `ai_classify`, máscaras de columna |
| **Data Quality Monitoring** | Activan Anomaly detection sobre un esquema y un monitor de Data Profiling con métricas de negocio | Data Quality Monitoring |
| **DQX Studio** | Despliegan DQX Studio como una Databricks App y crean reglas de calidad en una experiencia guiada | DQX y DQX Studio (Databricks Labs) |

La guía paso a paso está en una app web que ustedes mismos pueden desplegar (ver [La guía como app](#la-guía-como-app)). Este README es el mapa del repositorio.

## Las tareas

| # | Tarea | Material | Tiempo |
|---|---|---|---|
| 1 | Preparar el catálogo | `notebooks/01_setup_taller.py` | 15 min |
| 2 | Data Classification | `notebooks/02_clasificacion_datos.py` | 40 min |
| 3 | Data Quality Monitoring | `notebooks/03_monitoreo_calidad.py` y `04_alimentar_y_romper.py` | 60 min |
| 4 | DQX con código | `notebooks/05_dqx_reglas.py` | 35 min |
| 5 | Desplegar DQX Studio | `notebooks/06_desplegar_dqx_studio.py` | 30 min |
| 6 | Reglas de calidad en DQX Studio | La app DQX Studio y la guía | 60 min |
| 7 | Limpieza y siguientes pasos | La guía | 15 min |

Las tareas 2 a 4 son independientes entre sí. La 5 y la 6 van juntas, y la 6 es más interesante después de haber hecho la 4, porque comparan las mismas reglas escritas con código y con la interfaz.

## Antes de empezar

1. Una cuenta de [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (gratis, con un correo).
2. Un Git Folder con este repositorio: **Workspace > Create > Git folder**, URL `https://github.com/scaravacap/governance-dq-workshop`.
3. Cómputo **Serverless** en todos los notebooks. Es el único que ofrece Free Edition.
4. Ejecuten `notebooks/01_setup_taller` y esperen 2 a 3 minutos. Crea el catálogo `taller_gobierno` con cinco tablas y los errores sembrados.

## Los datos y los errores sembrados

Cinco tablas en `taller_gobierno.banco`, deterministas: si dos personas ejecutan el notebook, obtienen exactamente los mismos números.

| Tabla | Filas | Para qué |
|---|---|---|
| `clientes` | 5.000 | Datos personales y errores de calidad |
| `cuentas` | 7.500 | Número de cuenta (IBAN) y saldos |
| `transacciones` | 60.000 | Número de tarjeta, montos, canales |
| `prestamos` | 3.000 | 500 préstamos en 6 cortes mensuales, para el monitor de perfilamiento |
| `bitacora_contactos` | 2.000 | Texto libre con datos personales dentro de la nota |

Errores en `clientes` (la respuesta correcta que cada herramienta debe encontrar):

| Error | Filas |
|---|---|
| `cedula` nula o con el texto `N/A` | 169 (113 nulas y 56 con `N/A`) |
| `correo` sin arroba | 92 |
| `pais` con el código inválido `XX` | 52 |
| `ingreso_mensual` negativo | 53 |
| `fecha_alta` en el futuro | 29 |
| `id_cliente` repetido | 60 (30 identificadores, dos veces cada uno) |

En `prestamos` hay tres errores de fila (saldo mayor al desembolsado, tasa en cero y país nulo, 1% cada uno) y un **incidente de negocio**: el índice de mora a más de 90 días pasa de menos del 1% a cerca del 9% en el último corte. Ninguna regla de fila lo detecta porque cada préstamo es válido por separado. Solo aparece en la serie de tiempo del monitor.

## Qué cambia en Free Edition

Lo que verifiqué al probar el taller en Free Edition, y cómo lo resuelve cada notebook:

| Límite | Efecto | Solución en el taller |
|---|---|---|
| **Data Classification nativo no está disponible** | La API responde "not available in this workspace" | El notebook 02 lo comprueba. Si falta, ejecuta un escáner equivalente (expresiones regulares, una regla para la cédula y `ai_classify` para el texto libre) y deja las mismas etiquetas |
| **Un solo SQL warehouse** | El bundle de DQX Studio quiere crear el suyo y falla con `429 RESOURCE_EXHAUSTED` | El notebook 06 parcha el bundle para usar el warehouse que ya existe |
| **Máximo 3 apps** | Studio usa una | El notebook 06 revisa el cupo antes de desplegar |
| **Máximo 3 proyectos Lakebase** | Studio usa uno | El notebook 06 revisa el cupo antes de desplegar |
| **Solo cómputo serverless** | No hay clusters clásicos | Todos los notebooks corren en serverless. El CLI de Databricks se descarga dentro del notebook |
| **Grupos solo de workspace** | No hay grupos de cuenta | El notebook 02 crea `auditoria_pii` como grupo del workspace |
| **El job de Studio arranca lento** | El primer perfilado o la primera ejecución tarda varios minutos mientras el cómputo serverless se calienta | Avisen a los participantes. Las siguientes corridas son rápidas |
| **Data Profiling tarda en la primera corrida** | Entre 10 y 15 minutos | Hagan la tarea 4 mientras esperan |

## DQX Studio

[DQX Studio](https://github.com/databrickslabs/dqx) es una Databricks App de Databricks Labs, en **Beta**, donde se crean reglas de calidad sin código: se define una regla reutilizable, se aplica a las columnas de una tabla, pasa por aprobación, se ejecuta y se ven los registros que fallaron.

El notebook 06 despliega el release `studio-v0.1.0` desde el propio workspace:

1. Crea el service principal `dqx-studio-task-runner` y le da a quien despliega el rol `servicePrincipal.user` sobre él. Sin ese rol, el bundle no puede configurar un job que corra con esa identidad. El ID de la cuenta sale del metastore.
2. Descarga el Databricks CLI y el release desde GitHub, y autentica el CLI con la credencial del notebook.
3. Parcha el bundle de Free Edition (un solo warehouse).
4. Ejecuta `bundle deploy` y `bundle run`. Este último aplica la configuración de la app; un `apps deploy` directo se salta ese paso y deja la app sin conexión a Lakebase.
5. Otorga `USE CATALOG` y `CREATE SCHEMA` a la identidad de la app, y `USE CATALOG` al service principal de revisión y a todos los usuarios.
6. Comparte la app con el grupo `users` y verifica los pasos de configuración de Studio. El paso `app_sharing` puede quedar en `warning` porque Studio no puede leer con quién está compartida la app; es informativo.

Studio queda en modo abierto: cualquier usuario del workspace puede entrar. En un workspace empresarial se limita a un grupo con la variable `studio_user_group`.

## La guía como app

La carpeta raíz es una Databricks App (FastAPI y una página estática) que muestra la guía paso a paso, con progreso por tarea guardado en el navegador. No necesita recursos de Databricks para funcionar.

Para desplegarla en su workspace, desde un terminal con el [CLI de Databricks](https://docs.databricks.com/aws/en/dev-tools/cli/) autenticado:

```bash
databricks apps create guia-taller-gobierno
databricks sync . /Workspace/Users/<su-correo>/guia-taller-gobierno --exclude .git --exclude .databricks
databricks apps deploy guia-taller-gobierno --source-code-path /Workspace/Users/<su-correo>/guia-taller-gobierno
```

Para verla en local:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py        # http://localhost:8000
```

El contenido vive en `data/tasks.json`, que se genera con `python3 scripts/build_tasks.py`. Editen el script y regeneren el JSON, así evitan pelear con el escape de texto largo.

## Limpieza

Studio deja una app, una base Lakebase, un job y cuatro esquemas. Las instrucciones completas están en la tarea 7 de la guía. En resumen:

```sql
DROP SCHEMA IF EXISTS taller_gobierno.dqx_studio CASCADE;
DROP SCHEMA IF EXISTS taller_gobierno.dqx_studio_tmp CASCADE;
DROP SCHEMA IF EXISTS taller_gobierno.dqx_studio_genie CASCADE;
DROP SCHEMA IF EXISTS taller_gobierno.dqx_studio_demo CASCADE;
-- Para borrar todo el taller:
DROP CATALOG IF EXISTS taller_gobierno CASCADE;
```

Borren también la app `dqx-studio`, el job `dqx-studio-task-runner` y el proyecto Lakebase `dqx-studio-db`. Este último hay que borrarlo con `purge=true`: sin eso, Lakebase lo manda a una papelera y el nombre queda reservado, y un nuevo despliegue falla con "A soft-deleted project currently has this resource name".

## Notas para quien facilita el taller

- **Prueben el notebook 06 un día antes** en un workspace limpio. Es la pieza con más partes móviles y depende de GitHub, del CLI y de que el workspace tenga cupo de apps y de Lakebase.
- **Empiecen Data Profiling temprano.** Es la primera corrida más lenta. Si lanzan el notebook 03 al inicio de la sesión, el monitor ya tiene datos cuando lleguen a esa tarea.
- **Anomaly detection no mostrará alertas en un día de taller.** El modelo necesita varios días de historia. Lo que sí se ve en pantalla es qué tabla vigila y con qué cadencia. Si quieren una demostración con alerta, programen el notebook 04 en modo `normal` varios días antes y pásenlo a `pausa` la noche anterior.
- **El grupo `auditoria_pii` se crea en el workspace de cada participante.** Antes de agregarse al grupo ven asteriscos y después ven el valor completo, con unos minutos de caché.
- **Todo es sintético.** Ninguna cédula, tarjeta, cuenta ni persona es real.

## Estructura del repositorio

```
notebooks/            Los seis notebooks del taller
dqx_studio/           patch_bundle.py: el parche de Free Edition, como script independiente
scripts/build_tasks.py  Genera data/tasks.json
data/tasks.json       Contenido de la guía
frontend/             La página de la guía
main.py               API FastAPI y servidor estático
app.yaml              Arranque de la Databricks App
```

## Estado del material

Material de habilitación. DQX Studio está en Beta y puede cambiar. Verifiquen los resultados antes de usar cualquier regla en una decisión operacional.
