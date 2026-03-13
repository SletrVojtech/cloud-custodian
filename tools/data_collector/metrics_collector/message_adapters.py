from tools.data_collector.rabbitmq.message import IngestionMessage
from tools.data_collector.metrics_collector.message import MetricsPayload

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseCloudAdapter(ABC):
    """Downloaded metrics to RabbitMQ MetricsPayload message adapter class"""
    
    def __init__(self, raw_data: Dict[str, Any], **kwargs):
        """Kwargs for adding data not available in returned values."""
        self.raw_data = raw_data
        self.kwargs = kwargs 

    @abstractmethod
    def get_provider(self) -> str: pass

    @abstractmethod
    def get_resource_id(self) -> str: pass

    @abstractmethod
    def get_resource_type(self) -> str: pass

    @abstractmethod
    def get_resource_name(self) -> str: pass

    @abstractmethod
    def get_billing_account_id(self) -> str: pass

    @abstractmethod
    def get_metric_name(self) -> str: pass

    @abstractmethod
    def get_region_name(self) -> str: pass

    @abstractmethod
    def get_tags(self) -> Dict[str, str]: pass

    @abstractmethod
    def get_datapoints(self) -> List[Dict[str, Any]]:
        """Returns list of {'timestamp': t, 'value': v}"""
        pass

    def get_extras(self) -> Dict[str, Any]:
        """Special values based on resource type"""
        return {}

    def to_payloads(self) -> List[MetricsPayload]:
        """
        Assembles the payload
        """
        return MetricsPayload(
            provider=self.get_provider(),
            resource_id=self.get_resource_id(),
            resource_type=self.get_resource_type(),
            resource_name=self.get_resource_name(),
            metric_name=self.get_metric_name(),
            billing_account_id=self.get_billing_account_id(),
            region_name=self.get_region_name(),
            tags=self.get_tags(),
            datapoints=self.get_datapoints(),
            extras=self.get_extras()
        )


class AzureAdapter(BaseCloudAdapter):
    def get_provider(self) -> str:
        return "azure"

    def get_resource_id(self) -> str:
        return self.raw_data.get("id", "")

    def get_resource_type(self) -> str:
        return self.raw_data.get("type", "unknown")

    def get_resource_name(self) -> str:
        return self.raw_data.get("name", "unknown")

    def get_billing_account_id(self) -> str:
        # Extract Subscription ID from Resource ID
        res_id = self.get_resource_id()
        parts = res_id.split("/")
        if "subscriptions" in parts:
            idx = parts.index("subscriptions")
            if len(parts) > idx + 1:
                return parts[idx + 1]
        return "unknown"

    def get_region_name(self) -> str:
        return self.raw_data.get("location", "unknown")

    def get_tags(self) -> Dict[str, str]:
        return self.raw_data.get("tags", {})

    def _get_raw_metric_data(self):
        """Extracts metrics """
        metrics_dict = self.raw_data.get("c7n:metrics", {})
        if not metrics_dict:
            return None
        
        # Only one metric at a time. 
        raw_key = list(metrics_dict.keys())[0]
        return metrics_dict[raw_key]

    def get_metric_name(self) -> str:
        policy_name =  self.kwargs.get("policy_name", "azure_unknown").split("_", 1)
        if len(policy_name) > 1:
            return policy_name[1]
        else:
            return "unknown"

    def get_datapoints(self) -> List[Dict[str, Any]]:
        data = self._get_raw_metric_data()
        if not data:
            return []

        clean_datapoints = []
        try:
            timeseries_data = data["metrics_data"]["value"][0]["timeseries"][0]["data"]
            policy_aggregation = self.kwargs.get("policy_aggregation", "average")
            for point in timeseries_data:
                clean_datapoints.append({
                    "timestamp": point["time_stamp"],
                    "value": point.get(policy_aggregation, 0.0)
                })
        except (KeyError, IndexError):
            pass
            
        return clean_datapoints

    def get_extras(self) -> Dict[str, Any]:
        return {}




class AwsAdapter(BaseCloudAdapter):
    def get_provider(self) -> str:
        return "aws"

    def get_resource_id(self) -> str:
        # AWS doesn't have standardized ID name,
        # needs to be implemented per resource-type.
        return self.raw_data.get("InstanceId", self.raw_data.get("Id", "unknown"))

    def get_resource_type(self) -> str:
        return self.kwargs.get("resource_type", "aws_ec2")

    def get_tags(self) -> Dict[str, str]:
        # Parsing list of dictionaries: [{"Key": "x", "Value": "y"}]
        # to a single dictionary
        tags_list = self.raw_data.get("Tags", [])
        return {tag["Key"]: tag["Value"] for tag in tags_list}

    def get_resource_name(self) -> str:
        # Read from tags, otherwise ID
        tags = self.get_tags()
        return tags.get("Name", self.get_resource_id())

    def get_billing_account_id(self) -> str:
        return self.kwargs.get("account_id", "unknown_account")

    def get_region_name(self) -> str:
        return self.kwargs.get("region_name", "unknown")
    
    def _get_raw_metric_data(self):
        """Extract 'c7n.metrics' from raw data.
        Only one metric type is present.
        """
        metrics_dict = self.raw_data.get("c7n.metrics", {})
        if not metrics_dict:
            return []
        
        raw_key = list(metrics_dict.keys())[0]
        return metrics_dict[raw_key]
    
    def get_metric_name(self) -> str:
        policy_name =  self.kwargs.get("policy_name", "aws_unknown").split("_", 1)
        if len(policy_name) > 1:
            return policy_name[1]
        else:
            return "unknown"

    def get_datapoints(self) -> List[Dict[str, Any]]:
        data_list = self._get_raw_metric_data()
        if not data_list:
            return []

        clean_datapoints = []
        for point in data_list:
            policy_aggregation = self.kwargs.get("policy_aggregation", "Average")
            clean_datapoints.append({
                "timestamp": point.get("Timestamp"),
                "value": point.get(policy_aggregation, 0.0)
            })
            
        return clean_datapoints

    def get_extras(self) -> Dict[str, Any]:
        return {}

