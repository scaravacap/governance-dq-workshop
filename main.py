"""Taller de gobierno y calidad de datos.

FastAPI serves a static, customer-facing guide and a small JSON API with the
guided tasks. The app does not require Databricks resources at runtime.
"""

import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).parent
DATA_PATH = ROOT / "data" / "tasks.json"

with DATA_PATH.open("r", encoding="utf-8") as file:
    DATA = json.load(file)

TASKS = DATA["tasks"]
TASKS_BY_ID = {task["id"]: task for task in TASKS}
SUMMARY_KEYS = (
    "id",
    "index",
    "title",
    "subtitle",
    "product",
    "icon",
    "color",
    "estimatedMinutes",
    "mode",
)

app = FastAPI(title="Taller de gobierno y calidad de datos", version="1.0.0")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/workshop")
def get_workshop() -> dict:
    return DATA["workshop"]


@app.get("/api/tasks")
def list_tasks() -> list[dict]:
    return [{key: task[key] for key in SUMMARY_KEYS} for task in TASKS]


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    task = TASKS_BY_ID.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    return task


app.mount("/", StaticFiles(directory=ROOT / "frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
    )
