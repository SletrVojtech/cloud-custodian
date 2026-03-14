from pydantic import BaseModel, Field
from typing import Dict, Optional, List
from datetime import datetime

class CostPayload(BaseModel):
    # Entity identification
    provider: str
    account_id: str
    region_name: str
    resource_id: str
    resource_name: str
    resource_type: str
    tags: Dict[str, str] = Field(default_factory=dict)

    # Cost entry identification
    service_name: str
    service_category: str
    sku_price_id: str
    
    # Charge period
    charge_period_start: datetime
    charge_period_end: datetime
    
    # Costs
    billed_cost: float
    billing_currency: str

    

class CostBatchPayload(BaseModel):
    """Batch Message for RabbitMQ"""
    records: List[CostPayload]