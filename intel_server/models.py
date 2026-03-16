from pydantic import BaseModel
from datetime import datetime

class IntelSignal(BaseModel):
    signal_id: str
    timestamp: datetime
    entity_id: str
    reported_lat: float
    reported_lon: float
    signal_type: str = 'unknown'
    priority_level: int
    speed: int = -1
    distance: int = -1
