# export_metrics.py
import requests
import json
from pathlib import Path

BASE = "http://127.0.0.1:8000"
OUT = Path("output")
OUT.mkdir(exist_ok=True)

endpoints = {
    "hires_by_quarter_2021": "/metrics/hires-by-quarter-2021",
    "departments_above_mean_2021": "/metrics/departments-above-mean-2021"
}

for name, path in endpoints.items():
    url = BASE + path
    print("Requesting", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    out_file = OUT / f"metrics_{name}.json"
    with out_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print("Saved", out_file, "rows:", len(data))
