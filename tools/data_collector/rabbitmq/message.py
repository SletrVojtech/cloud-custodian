from pydantic import BaseModel, Field
from typing import Any, Dict
from datetime import datetime, timezone
import uuid



class IngestionMessage(BaseModel):
    """
    Base class for RabbitMQ  messages.
    To deliver data to the DB collector, inherit from this class
    and load your module-specific data into the payload dictionary.
    """
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_module: str 
    payload: Dict[str, Any]