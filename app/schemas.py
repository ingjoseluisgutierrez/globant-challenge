from pydantic import BaseModel, Field
from typing import List, Dict, Any

class BatchPayload(BaseModel):
    rows: List[Dict[str, Any]] = Field(..., min_items=1, max_items=10000)
