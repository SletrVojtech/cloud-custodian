import json
from tools.data_collector.cost_collector.message import CostPayload


class FocusCostAdapter:
    """FOCUS 1.2 adapter class"""
    
    def __init__(self, row: dict):
        self.row = row

    def get_provider(self) -> str:
        provider = str(self.row.get('ProviderName', '')).lower()
        if 'aws' in provider or 'amazon' in provider:
            return 'aws'
        elif 'microsoft' in provider or 'azure' in provider:
            return 'azure'
        return 'unknown'

    def get_account_id(self) -> str:
        acc_id = str(self.row.get('SubAccountId', ''))
        if "subscriptions/" in acc_id.lower():
            return acc_id.split("/")[-1]
        return acc_id

    def get_resource_id(self) -> str:
        res_id = self.row.get('resource_id')
        if not res_id:
            service = self.row.get('ServiceName', 'general')
            return str(service)
        return str(res_id)

    def get_tags(self) -> dict:
        tags_raw = self.row.get('Tags')
        if not tags_raw:
            return {}
        if isinstance(tags_raw, dict):
            return tags_raw
        if isinstance(tags_raw, str):
            try: return json.loads(tags_raw)
            except: return {}
        return {}


    def to_payload(self) -> CostPayload:
        return CostPayload(
            provider=self.get_provider(),
            account_id=self.get_account_id(),
            region_name=str(self.row.get('RegionName', 'Unknown')),
            
            resource_id=self.get_resource_id(),
            resource_name=str(self.row.get('ResourceName', self.get_resource_id())),
            resource_type=str(self.row.get('ResourceType', 'Unknown')),
            tags=self.get_tags(),
            service_name=str(self.row.get('ServiceName', 'Unknown')),
            service_category=str(self.row.get('ServiceCategory', 'Unknown')),
            sku_price_id=str(self.row.get('SkuPriceId', 'Unknown')),
            
            charge_period_start=self.row.get('charge_period_start'),
            charge_period_end=self.row.get('charge_period_end'),
            
            billed_cost=float(self.row.get('BilledCost', 0.0) or 0.0),
            billing_currency=str(self.row.get('BillingCurrency', 'EUR')),
            
        )