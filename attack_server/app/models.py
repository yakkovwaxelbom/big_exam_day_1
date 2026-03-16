from pydantic import BaseModel
from datetime import datetime

class AttackReport(BaseModel):
    attack_id: str
    timestamp: datetime
    entity_id: str
    weapon_type: str = 'unknown'

