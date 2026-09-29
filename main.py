"""FastAPI 仅保留最小存活检查；本地端到端流程见 ``demo.py``。"""

from fastapi import FastAPI

app = FastAPI(title="audit-event-pipeline")


@app.get("/")
def read_root():
    return {"status": "ok", "service": "audit-event-pipeline"}
