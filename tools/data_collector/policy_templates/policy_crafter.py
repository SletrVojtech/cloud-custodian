from abc import ABC, abstractmethod

class PolicyCrafter(ABC):
    """Abstract class for supported providers"""
    
    @abstractmethod
    def craft(self, resource, metric, period):
        pass

class AWSPolicyCrafter(PolicyCrafter):
    def craft(self,resource: str, metric: str, period: str = 'PT5M'):
        POLICY_DATA = {
        'name': 'aws_resource_loader_'+metric.replace(' ', '_'), 
        'resource': resource, 
        'filters': [{
             'type': 'metrics', 
             'name': metric, 
             'days': 0.5, 
             'period': 300, 
             'value': 0, 
             'op': 'ge'
             }]
        }
        return POLICY_DATA

class AzurePolicyCrafter(PolicyCrafter):
    def craft(self, resource: str, metric: str, period: str = 'PT5M'):
        POLICY_DATA = {
            'name': 'azure_resource_loader_'+metric.replace(' ', '_'),
            'resource': resource,
            'filters': [{
                'type': 'metric',
                'metric': metric, 
                'aggregation': 'average', 
                'op': 'ge', 
                'threshold': 0, 
                'timeframe': 1, 
                'interval': period
                },]
        }
        return POLICY_DATA

class CrafterFactory:
    _mapping = {
        'aws': AWSPolicyCrafter(),
        'azure': AzurePolicyCrafter()
    }

    @staticmethod
    def get_crafter(resource_name: str) -> PolicyCrafter:
        # Get the resource prefix for provider mapping
        prefix = resource_name.split('.')[0]
        crafter = CrafterFactory._mapping.get(prefix)
        
        if not crafter:
            raise ValueError(f"Provider '{prefix}' isn't supported..")
        return crafter