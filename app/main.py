# app/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from io import StringIO
from .db import engine, Base, get_db
from . import models, crud, utils, schemas
import csv

app = FastAPI(title="Globant Data Engineering Challenge - API")

def _rows_to_csv(rows, fieldnames=None):
    buf = StringIO()
    if not rows:
        fieldnames = fieldnames or []
    else:
        fieldnames = fieldnames or list(rows[0].keys())
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, "") for k in fieldnames})
    buf.seek(0)
    return buf



@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


@app.get("/")
async def root():
    return {"status": "ok", "message": "Hola Jose Luis — API lista"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
    

@app.post("/upload-csv/{table}")
async def upload_csv(table: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    table = table.lower()
    allowed = {"departments", "jobs", "hired_employees"}
    if table not in allowed:
        raise HTTPException(status_code=400, detail=f"table must be one of {allowed}")

    content = await file.read()
    required_map = {
        "departments": ["id", "department"],
        "jobs": ["id", "job"],
        "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
    }
    try:
        rows = utils.parse_csv_bytes(content, required_columns=required_map[table])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not (1 <= len(rows) <= 10000):
        raise HTTPException(status_code=400, detail="rows must be between 1 and 10000 per request")

    try:
        inserted = crud.batch_insert(table, rows, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"insert error: {e}")

    return {"inserted": inserted}


@app.post("/batch-insert/{table}")
def batch_insert(table: str, payload: schemas.BatchPayload, db: Session = Depends(get_db)):
    table = table.lower()
    allowed = {"departments", "jobs", "hired_employees"}
    if table not in allowed:
        raise HTTPException(status_code=400, detail=f"table must be one of {allowed}")
    rows = payload.rows

    if table == "departments":
        req = {"id", "department"}
    elif table == "jobs":
        req = {"id", "job"}
    else:
        req = {"id", "name", "datetime", "department_id", "job_id"}

    if len(rows) == 0:
        raise HTTPException(status_code=400, detail="no rows provided")
    first_keys = set(rows[0].keys())
    missing = req - first_keys
    if missing:
        raise HTTPException(status_code=400, detail=f"missing columns: {missing}")

    if not (1 <= len(rows) <= 10000):
        raise HTTPException(status_code=400, detail="rows must be between 1 and 10000 per request")

    try:
        inserted = crud.batch_insert(table, rows, db)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"insert error: {e}")
    return {"inserted": inserted}


@app.get("/metrics/hires-by-quarter-2021")
def hires_by_quarter_2021(
    dbs: Session = Depends(get_db),
    format: str = Query("json", pattern="^(json|csv)$")
):
    try:
        rows = crud.hires_by_quarter_2021(dbs)
        if format == "csv":
            fieldnames = ["department", "job", "q1", "q2", "q3", "q4"]
            buf = _rows_to_csv(rows, fieldnames=fieldnames)
            headers = {"Content-Disposition": 'attachment; filename="hires_by_quarter_2021.csv"'}
            return StreamingResponse(buf, media_type="text/csv", headers=headers)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"metrics error: {e}")



@app.get("/metrics/departments-above-mean-2021")
def departments_above_mean_2021(
    dbs: Session = Depends(get_db),
    format: str = Query("json", pattern="^(json|csv)$")
):
    try:
        rows = crud.departments_above_mean_2021(dbs)
        if format == "csv":
            fieldnames = ["id", "department", "hired"]
            buf = _rows_to_csv(rows, fieldnames=fieldnames)
            headers = {"Content-Disposition": 'attachment; filename="departments_above_mean_2021.csv"'}
            return StreamingResponse(buf, media_type="text/csv", headers=headers)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"metrics error: {e}")

