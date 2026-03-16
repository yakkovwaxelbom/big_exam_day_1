from pydantic import BaseModel
from datetime import datetime

class DamageReport(BaseModel):
    attack_id: str
    timestamp: datetime
    result: str

