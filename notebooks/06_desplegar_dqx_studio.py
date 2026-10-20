# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Desplegar DQX Studio en su workspace
# MAGIC
# MAGIC DQX Studio es una aplicación web (Databricks App) donde se crean reglas de calidad sin escribir código: se define una regla, se aplica a una tabla, se aprueba, se ejecuta y se ven los resultados. Es el mismo motor DQX del notebook 05, con una experiencia guiada encima.
# MAGIC
# MAGIC Este notebook hace el despliegue completo desde el propio workspace, sin instalar nada en su computador:
# MAGIC
# MAGIC 1. Crea el service principal que ejecuta las revisiones de calidad y se da a ustedes el rol para usarlo.
# MAGIC 2. Descarga el release `studio-v0.1.0` de DQX Studio y el Databricks CLI.
# MAGIC 3. Ajusta el bundle a Free Edition, que permite un solo SQL warehouse y ya existe el Serverless Starter.
# MAGIC 4. Despliega el bundle (app, base Lakebase, job de revisión, esquemas) y arranca la app.
# MAGIC 5. Otorga los permisos sobre el catálogo y verifica la configuración.
# MAGIC
# MAGIC **Cómputo:** serverless. **Tiempo:** 10 a 15 minutos, casi todo es la creación de la base Lakebase y el arranque de la app.
# MAGIC
# MAGIC **Antes de empezar:** Free Edition permite 3 apps por workspace y DQX Studio usa una. Ejecuten antes `01_setup_taller`, porque Studio guarda sus tablas en el catálogo del taller.
# MAGIC
# MAGIC **Estado del producto:** DQX Studio está en Beta. Aquí lo usamos en modo abierto, donde cualquier usuario del workspace puede entrar. En un workspace empresarial se limita a un grupo.

# COMMAND ----------

dbutils.widgets.text("catalogo", "taller_gobierno", "Catálogo del taller")
dbutils.widgets.text("version_studio", "studio-v0.1.0", "Release de DQX Studio")
dbutils.widgets.text("nombre_app", "dqx-studio", "Nombre de la app")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse (vacío = el primero)")

CAT = dbutils.widgets.get("catalogo")
VERSION = dbutils.widgets.get("version_studio")
APP = dbutils.widgets.get("nombre_app")
SP_NOMBRE = "dqx-studio-task-runner"

# COMMAND ----------

# MAGIC %md ## 1. Verificar el punto de partida

# COMMAND ----------

import json
import os
import platform
import re
import subprocess
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
yo = w.current_user.me().user_name
print("Usuario:", yo)
print("Workspace:", w.config.host)

assert spark.sql(f"SHOW CATALOGS LIKE '{CAT}'").count() == 1, f"No existe el catálogo {CAT}. Ejecuten primero 01_setup_taller."

bodega = w.warehouses.get(dbutils.widgets.get("warehouse_id")) if dbutils.widgets.get("warehouse_id") else next(iter(w.warehouses.list()), None)
assert bodega, "No hay ningún SQL warehouse. Free Edition trae el Serverless Starter Warehouse."
print("SQL warehouse:", bodega.name, bodega.id)

proyectos_lb = [
    p["project_id"] for p in w.api_client.do("GET", "/api/2.0/postgres/projects").get("projects", []) if not p.get("delete_time")
]
print("Proyectos Lakebase:", proyectos_lb)
if len(proyectos_lb) >= 3 and "dqx-studio-db" not in proyectos_lb:
    raise RuntimeError("Free Edition permite 3 proyectos Lakebase y ya tienen 3. Borren uno (con purge) antes de continuar.")

apps_existentes = [a.name for a in w.apps.list()]
print("Apps actuales:", apps_existentes)
if len(apps_existentes) >= 3 and APP not in apps_existentes:
    raise RuntimeError("Free Edition permite 3 apps y ya tienen 3. Borren una antes de continuar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. El service principal que ejecuta las revisiones
# MAGIC
# MAGIC Studio lanza un job serverless para cada perfilado y cada ejecución de reglas. Ese job corre con una identidad propia, un service principal, para que los permisos del que revisa no dependan de quién hizo clic.
# MAGIC
# MAGIC Para que el bundle pueda configurar un job con esa identidad, quien despliega necesita el rol `servicePrincipal.user` sobre ella. Ese rol vive a nivel de cuenta, así que lo otorgamos por la API de access control. El ID de la cuenta sale del metastore del workspace.

# COMMAND ----------

sps = list(w.service_principals.list(filter=f'displayName eq "{SP_NOMBRE}"'))
sp = sps[0] if sps else w.service_principals.create(display_name=SP_NOMBRE, active=True)
SP_APP_ID = sp.application_id
print("Service principal:", SP_NOMBRE, SP_APP_ID)

cuenta_id = w.api_client.do("GET", "/api/2.1/unity-catalog/metastores")["metastores"][0]["metastore_account_id"]
regla_nombre = f"accounts/{cuenta_id}/servicePrincipals/{SP_APP_ID}/ruleSets/default"
rs = w.api_client.do("GET", "/api/2.0/preview/accounts/access-control/rule-sets", query={"name": regla_nombre, "etag": ""})
principal = f"users/{yo}"
ya_tiene = any(
    r["role"] == "roles/servicePrincipal.user" and principal in r.get("principals", [])
    for r in rs.get("grant_rules", [])
)
if ya_tiene:
    print("Ya tienen el rol servicePrincipal.user")
else:
    reglas = rs.get("grant_rules", []) + [{"role": "roles/servicePrincipal.user", "principals": [principal]}]
    w.api_client.do(
        "PUT",
        "/api/2.0/preview/accounts/access-control/rule-sets",
        body={"name": regla_nombre, "rule_set": {"name": regla_nombre, "etag": rs["etag"], "grant_rules": reglas}},
    )
    print("Rol servicePrincipal.user otorgado a", yo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Descargar el CLI y el release de DQX Studio
# MAGIC
# MAGIC El despliegue usa Databricks Asset Bundles, que se ejecutan con el CLI. Lo descargamos de GitHub (el cómputo serverless de Free Edition alcanza GitHub) y lo autenticamos con la credencial de este notebook.

# COMMAND ----------

CLI_VERSION = "1.20.0"
arq = {"x86_64": "amd64", "aarch64": "arm64"}[platform.machine()]
TRABAJO = Path("/tmp/dqx_studio_deploy")
TRABAJO.mkdir(exist_ok=True)


def sh(cmd, cwd=None, env=None, check=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd, env=env)
    salida = (r.stdout + r.stderr).strip()
    if check and r.returncode != 0:
        raise RuntimeError(f"Falló: {cmd}\n{salida[-3000:]}")
    return salida


if not (TRABAJO / "databricks").exists():
    sh(f"curl -sL -o cli.zip https://github.com/databricks/cli/releases/download/v{CLI_VERSION}/databricks_cli_{CLI_VERSION}_linux_{arq}.zip && unzip -o -q cli.zip databricks", cwd=TRABAJO)
print(sh(f"{TRABAJO}/databricks --version"))

repo = TRABAJO / "dqx"
if not repo.exists():
    sh(f"git clone --quiet --depth 1 --branch {VERSION} https://github.com/databrickslabs/dqx.git dqx", cwd=TRABAJO)
print("Release:", sh("git describe --tags", cwd=repo))

# La credencial del notebook dura poco: se vuelve a pedir en cada llamada al CLI.
def cli(args, cwd=None, check=True):
    token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_CLIENT_")}
    env.update(DATABRICKS_HOST=w.config.host, DATABRICKS_TOKEN=token)
    return sh(f"{TRABAJO}/databricks {args}", cwd=cwd, env=env, check=check)


print(cli("current-user me -o json | head -c 120"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ajustar el bundle a Free Edition
# MAGIC
# MAGIC El bundle del release siempre crea su propio SQL warehouse. Free Edition permite uno solo, así que el despliegue original falla con `429 RESOURCE_EXHAUSTED`. El parche quita ese recurso y apunta la app y el job al warehouse que ya tienen, mediante una variable nueva `warehouse_id`.
# MAGIC
# MAGIC En un workspace empresarial no hace falta este paso.

# COMMAND ----------

yml = repo / "app" / "databricks.yml"
texto = yml.read_text()
if 'description: "Existing SQL warehouse' not in texto:
    texto = texto.replace(
        "variables:\n",
        'variables:\n  warehouse_id:\n    description: "Existing SQL warehouse (Free Edition allows only one)"\n    default: ""\n',
        1,
    )
    texto, n = re.subn(r"  # Bundle-managed SQL warehouse\..*?(?=  # Stateful storage\.)", "", texto, count=1, flags=re.S)
    assert n == 1, "No se encontró el bloque del warehouse administrado. ¿Cambió el release?"
    texto = texto.replace("${resources.sql_warehouses.dqx_sql_warehouse.id}", "${var.warehouse_id}")
    assert "dqx_sql_warehouse" not in texto
    yml.write_text(texto)
    print("Bundle parcheado")
else:
    print("El bundle ya estaba parcheado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Desplegar
# MAGIC
# MAGIC Tres variables importan:
# MAGIC
# MAGIC | Variable | Valor | Por qué |
# MAGIC |---|---|---|
# MAGIC | `catalog_name` | el catálogo del taller | Studio crea sus esquemas ahí (`dqx_studio`, `dqx_studio_tmp`, `dqx_studio_genie`, `dqx_studio_demo`) |
# MAGIC | `studio_user_group` y `studio_uc_principal` | `users` y `account users` | Modo abierto: todo el workspace puede usar Studio |
# MAGIC | `dqx_service_principal_application_id` | el service principal del paso 2 | Identidad del job de revisión |
# MAGIC
# MAGIC El primer despliegue tarda entre 5 y 10 minutos porque aprovisiona una base Lakebase.

# COMMAND ----------

VARS = (
    f"--var catalog_name={CAT} "
    f"--var warehouse_id={bodega.id} "
    f"--var dqx_service_principal_application_id={SP_APP_ID} "
    f"--var app_name={APP} "
    "--var studio_user_group=users "
    "--var 'studio_uc_principal=account users'"
)
app_dir = repo / "app"

# Si borraron Studio antes, Lakebase conserva el proyecto en una papelera ("soft delete") y el nombre
# queda ocupado: el despliegue falla con "A soft-deleted project currently has this resource name".
# Lo purgamos para poder reutilizar el nombre.
PROYECTO_LB = "dqx-studio-db"
proyectos = w.api_client.do("GET", "/api/2.0/postgres/projects", query={"show_deleted": "true"}).get("projects", [])
borrado = any(p["project_id"] == PROYECTO_LB and p.get("delete_time") for p in proyectos)
if borrado:
    print("Purgando el proyecto Lakebase borrado", PROYECTO_LB)
    w.api_client.do("DELETE", f"/api/2.0/postgres/projects/{PROYECTO_LB}", query={"purge": "true"})

print(cli(f"bundle validate -t release {VARS}", cwd=app_dir)[-600:])

# COMMAND ----------

print(cli(f"bundle deploy -t release --auto-approve {VARS}", cwd=app_dir)[-1500:])

# COMMAND ----------

# MAGIC %md
# MAGIC `bundle run` aplica la configuración de la app (variables de entorno, recursos enlazados) y la despliega. Un `apps deploy` directo se salta ese paso y la app queda sin conexión a Lakebase.

# COMMAND ----------

print(cli(f"bundle run dqx-studio -t release {VARS}", cwd=app_dir)[-1500:])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Permisos sobre el catálogo
# MAGIC
# MAGIC El bundle no administra el catálogo, que ya existía. Studio exige tres permisos antes de arrancar su configuración:
# MAGIC
# MAGIC - La identidad de la app necesita `USE CATALOG` y `CREATE SCHEMA`.
# MAGIC - El service principal de revisión necesita `USE CATALOG`.
# MAGIC - Los usuarios necesitan `USE CATALOG` para ver sus tablas desde Studio.

# COMMAND ----------

app = w.apps.get(APP)
app_sp = app.service_principal_client_id
print("Identidad de la app:", app_sp)

for principal, privilegios in [
    (app_sp, "USE CATALOG, CREATE SCHEMA"),
    (SP_APP_ID, "USE CATALOG"),
    ("account users", "USE CATALOG"),
]:
    spark.sql(f"GRANT {privilegios} ON CATALOG {CAT} TO `{principal}`")
    print("GRANT", privilegios, "->", principal)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compartir la app
# MAGIC
# MAGIC Studio revisa con quién está compartida la app. En modo abierto la compartimos con el grupo `users`, que contiene a todos los usuarios del workspace.

# COMMAND ----------

from databricks.sdk.service.apps import AppAccessControlRequest, AppPermissionLevel

w.apps.update_permissions(
    app_name=APP,
    access_control_list=[AppAccessControlRequest(group_name="users", permission_level=AppPermissionLevel.CAN_USE)],
)
print("Grupo users con permiso CAN_USE sobre", APP)

# COMMAND ----------

# MAGIC %md ## 7. Verificar la configuración de Studio

# COMMAND ----------

def api_studio(metodo, ruta, cuerpo=None):
    import urllib.error
    import urllib.request

    # Justo después del despliegue la app puede responder 502 unos minutos mientras arranca y migra Lakebase.
    for intento in range(40):
        token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
        req = urllib.request.Request(
            app.url.rstrip("/") + ruta,
            method=metodo,
            data=json.dumps(cuerpo).encode() if cuerpo is not None else None,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read() or "{}")
        except urllib.error.HTTPError as e:
            if e.code not in (502, 503, 504):
                raise
        time.sleep(15)
    raise TimeoutError("La app no respondió en 10 minutos")


try:
    api_studio("GET", "/api/v1/setup/status")  # espera a que la app responda
    api_studio("POST", "/api/v1/setup/reconcile", {})
    time.sleep(10)
    informe = api_studio("GET", "/api/v1/setup/status")["report"]
    print("Estado general:", informe["state"])
    for paso in informe["steps"]:
        print(f"{paso['state']:>16}  {paso['id']}  {paso['summary']}")
except Exception as e:  # noqa: BLE001
    print("No pude consultar el estado desde el notebook:", str(e)[:200])
    print("Abran la app y usen Verify again en la pantalla de configuración.")

print("\nAbran DQX Studio aquí:", app.url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Primer ingreso
# MAGIC
# MAGIC 1. Abran la URL de arriba (o **Compute > Apps > dqx-studio**).
# MAGIC 2. Si la pantalla de configuración sigue pendiente, usen **Verify again** hasta que todos los pasos estén en verde.
# MAGIC 3. Como administradores del workspace ya tienen todos los roles de Studio (autor, aprobador y visor), así que pueden recorrer el ciclo completo.
# MAGIC
# MAGIC Sigan con el laboratorio guiado: **Tarea 5** en la guía del taller.
# MAGIC
# MAGIC **Limpieza al terminar:** Studio deja una app, una base Lakebase, un job y cuatro esquemas. Las instrucciones para borrarlos están en el `README.md`, sección Limpieza.
