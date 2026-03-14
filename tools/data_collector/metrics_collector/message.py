from pydantic import BaseModel
from typing import Any, Dict, List

class MetricsPayload(BaseModel):
    provider: str
    resource_id: str
    resource_type: str
    resource_name: str
    metric_name: str
    metric_period: int
    billing_account_id: str
    region_name: str
    tags: Dict[str, str]
    datapoints: List[Dict[str, Any]]
    extras: Dict[str, Any]

