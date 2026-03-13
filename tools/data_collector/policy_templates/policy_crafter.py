from abc import ABC, abstractmethod
import string

class PolicyCrafter(ABC):
    """Abstract class for supported providers"""

    def craft_name(self, resource, metric ):
        # Based on https://www.digitalocean.com/community/tutorials/python-remove-spaces-from-string
        s = resource + '_' + metric
        replacements = str.maketrans(
            {" ": "_", ".": "_", "-": "_", "/": "_", "\\": "_"}
            | {ord(c): None for c in string.whitespace})
        return s.translate(replacements)
            
    @abstractmethod
    def craft(self, resource, metric, period):
        pass

class AWSPolicyCrafter(PolicyCrafter):
    def _get_period(period: str):
        """
        Based on allowed granularity for Azure policies in c7n_azure.filters.schema
        Enumerates to seconds, default is 5 minutes.
        """
        period_dict = {
            'PT5M': 300,
            'PT15M': 900,
            'PT30M': 1800,
            'PT1H': 3600,
            'PT6H': 21600,
            'PT12H': 43200,
            'P1D': 86400,
        }
        return period_dict.get(period, 300)


    def craft(self,resource: str, metric: str, period: str = 'PT5M'):
        POLICY_DATA = {
        'name': self.craft_name(resource,metric), 
        'resource': resource, 
        'filters': [{
             'type': 'metrics', 
             'name': metric, 
             'days': 0.5, 
             'period': self._get_period(period), 
             'value': 0,
             'missing-value': 0,
             'statistics': 'Average',
             'op': 'ge'
             }]
        }
        return POLICY_DATA

class AzurePolicyCrafter(PolicyCrafter):
    def craft(self, resource: str, metric: str, period: str = 'PT5M'):
        POLICY_DATA = {
            'name':  self.craft_name(resource,metric),
            'resource': resource,
            'filters': [{
                'type': 'metric',
                'metric': metric, 
                'aggregation': 'average', 
                'op': 'ge', 
                'threshold': 0, 
                'timeframe': 1, 
                'interval': period,
                'no_data_action': 'to_zero'
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