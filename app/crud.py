# app/crud.py
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from datetime import datetime
from dateutil import parser as dateparser
from . import models

SENTINEL_DATETIME_ISO = "9999-12-31T23:59:59Z"


def batch_insert(table: str, rows: list, db: Session):
    try:
        with db.begin():  # transacción atómica
            if table == "departments":
                # Espera rows con keys: id, department
                for idx, r in enumerate(rows, start=1):
                    id_raw = (r.get("id") or "").strip()
                    name = (r.get("department") or "").strip()
                    id_ = int(id_raw) if id_raw != "" else 0
                    # merge para upsert-like (si existe actualiza, si no inserta)
                    dept = models.Department(id=id_, department=name)
                    db.merge(dept)

            elif table == "jobs":
                # Espera rows con keys: id, job
                for idx, r in enumerate(rows, start=1):
                    id_raw = (r.get("id") or "").strip()
                    job_name = (r.get("job") or "").strip()
                    id_ = int(id_raw) if id_raw != "" else 0
                    job = models.Job(id=id_, job=job_name)
                    db.merge(job)

            elif table == "hired_employees":
                # Espera rows con keys: id, name, datetime, department_id, job_id
                for idx, r in enumerate(rows, start=1):
                    id_raw = (r.get("id") or "").strip()
                    name = (r.get("name") or "").strip()
                    dt_raw = (r.get("datetime") or "").strip()
                    dept_raw = (r.get("department_id") or "").strip()
                    job_raw = (r.get("job_id") or "").strip()

                    # datetime: si está vacío usamos el sentinel
                    if dt_raw == "":
                        try:
                            hire_dt = dateparser.isoparse(SENTINEL_DATETIME_ISO)
                        except Exception:
                            # fallback si parser fallara inesperadamente
                            hire_dt = datetime(9999, 12, 31, 23, 59, 59)
                    else:
                        try:
                            hire_dt = dateparser.isoparse(dt_raw)
                        except Exception as e:
                            raise ValueError(f"Row {idx}: datetime inválido -> {dt_raw}. Error: {e}")

                    # convertir numéricos: si están vacíos -> 0
                    try:
                        dept_id = int(dept_raw) if dept_raw != "" else 0
                    except Exception:
                        raise ValueError(f"Row {idx}: department_id inválido -> {dept_raw}")

                    try:
                        job_id = int(job_raw) if job_raw != "" else 0
                    except Exception:
                        raise ValueError(f"Row {idx}: job_id inválido -> {job_raw}")

                    # construir objeto HiredEmployee
                    if id_raw != "":
                        # si id proporcionado, usar merge (upsert)
                        try:
                            id_ = int(id_raw)
                        except Exception:
                            raise ValueError(f"Row {idx}: id inválido -> {id_raw}")
                        he = models.HiredEmployee(
                            id=id_,
                            name=name,
                            hire_datetime=hire_dt,
                            department_id=dept_id,
                            job_id=job_id
                        )
                        db.merge(he)
                    else:
                        # sin id -> dejar que DB genere la PK
                        he = models.HiredEmployee(
                            name=name,
                            hire_datetime=hire_dt,
                            department_id=dept_id,
                            job_id=job_id
                        )
                        db.add(he)
            else:
                raise ValueError(f"Unknown table: {table}")

        # commit implícito por with db.begin()
        return len(rows)

    except (KeyError, ValueError) as e:
        # rollback ya manejado por db.begin() si ocurre excepción, pero por seguridad cerramos
        try:
            db.rollback()
        except Exception:
            pass
        # Re-lanzar la excepción para que el endpoint capture y devuelva el detalle
        raise

    except SQLAlchemyError as e:
        try:
            db.rollback()
        except Exception:
            pass
        # Para debugging/la API, re-lanzamos la excepción
        raise


def hires_by_quarter_2021(db: Session):
    sql = text("""
    SELECT d.department, j.job,
      SUM(CASE WHEN EXTRACT(quarter FROM he.hire_datetime AT TIME ZONE 'UTC') = 1 THEN 1 ELSE 0 END) AS q1,
      SUM(CASE WHEN EXTRACT(quarter FROM he.hire_datetime AT TIME ZONE 'UTC') = 2 THEN 1 ELSE 0 END) AS q2,
      SUM(CASE WHEN EXTRACT(quarter FROM he.hire_datetime AT TIME ZONE 'UTC') = 3 THEN 1 ELSE 0 END) AS q3,
      SUM(CASE WHEN EXTRACT(quarter FROM he.hire_datetime AT TIME ZONE 'UTC') = 4 THEN 1 ELSE 0 END) AS q4
    FROM hired_employees he
    JOIN departments d ON he.department_id = d.id
    JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(year FROM he.hire_datetime AT TIME ZONE 'UTC') = 2021
    GROUP BY d.department, j.job
    ORDER BY d.department ASC, j.job ASC;
    """)
    result = db.execute(sql)
    try:
        rows = result.mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return [dict(row) for row in result.fetchall()]


def departments_above_mean_2021(db: Session):
    sql = text("""
    WITH hires AS (
      SELECT d.id, d.department, COUNT(*) AS hired
      FROM hired_employees he
      JOIN departments d ON he.department_id = d.id
      WHERE EXTRACT(year FROM he.hire_datetime AT TIME ZONE 'UTC') = 2021
      GROUP BY d.id, d.department
    ), mean_cte AS (
      SELECT AVG(hired) AS mean_hires FROM hires
    )
    SELECT h.id, h.department, h.hired
    FROM hires h, mean_cte m
    WHERE h.hired > m.mean_hires
    ORDER BY h.hired DESC;
    """)
    result = db.execute(sql)
    try:
        rows = result.mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return [dict(row) for row in result.fetchall()]

