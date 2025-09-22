import csv
from io import StringIO
from typing import List, Dict

def parse_csv_bytes(b: bytes, required_columns: List[str] = None) -> List[Dict[str, str]]:
    s = b.decode('utf-8-sig')
    reader = csv.DictReader(StringIO(s))
    rows = []
    for idx, row in enumerate(reader, start=1):
        # Normalizar keys y values
        normalized = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items()}
        rows.append(normalized)

    if required_columns:
        header = rows[0].keys() if rows else []
        missing = [c for c in required_columns if c not in header]
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
    return rows
