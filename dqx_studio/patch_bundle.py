#!/usr/bin/env python3
"""Adapta el bundle de DQX Studio a Databricks Free Edition.

Free Edition permite un solo SQL warehouse y ya existe el Serverless Starter, asi que
el bundle original (que siempre crea el suyo) falla con 429 RESOURCE_EXHAUSTED.
Este parche quita el recurso `sql_warehouses` y apunta la app y el dashboard al
warehouse existente mediante la variable `warehouse_id`.

Uso: python3 patch_bundle.py <ruta a dqx/app/databricks.yml>
"""
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()
if "warehouse_id:\n    description: \"Existing" in text:
    sys.exit("Ya parcheado")

# 1. Variable nueva
text = text.replace(
    "variables:\n",
    "variables:\n  warehouse_id:\n    description: \"Existing SQL warehouse (Free Edition allows only one)\"\n    default: \"\"\n",
    1,
)
# 2. Quitar el warehouse administrado por el bundle (bloque completo)
text, n = re.subn(
    r"  # Bundle-managed SQL warehouse\..*?(?=  # Stateful storage\.)", "", text, count=1, flags=re.S
)
assert n == 1, "No se encontro el bloque sql_warehouses"
# 3. Apuntar referencias al warehouse existente
text = text.replace("${resources.sql_warehouses.dqx_sql_warehouse.id}", "${var.warehouse_id}")
assert "dqx_sql_warehouse" not in text
path.write_text(text)
print("OK: bundle parcheado")
