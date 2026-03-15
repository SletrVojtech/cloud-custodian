import json
import logging
from psycopg2.extras import execute_values
from pydantic import ValidationError
from tools.data_collector.cost_collector.message import CostBatchPayload


log = logging.getLogger('CostsProcessor')

class CostsProcessor:

    def __init__(self, db_conn):
        self.db = db_conn
        self.cursor = self.db.cursor()

    def process(self, envelope):
        body = envelope.payload
        try:
            batch = CostBatchPayload.model_validate_json(body)
        except ValidationError as e:
            log.error(f"Invalid payload: {e}")
            raise

        if not batch.records:
            return

        # Find all EntityIDs
        entity_map = self._resolve_entities_bulk(batch.records)

        # Prepare bulk insert
        cost_values = []
        for record in batch.records:
            entity_id = entity_map.get(record.resource_id)
            if not entity_id:
                log.warning(f"Couldn't find entity for resource: {record.resource_id}")
                continue

            cost_values.append((
                entity_id,
                record.billed_cost,
                record.billing_currency,
                record.charge_period_start,
                record.charge_period_end,
                record.service_category,
                record.service_name,
                record.sku_price_id,
            ))

        if cost_values:
            self._insert_costs_bulk(cost_values)
            log.info(f"Batch {batch.batch_id}: Successfully inserted {len(cost_values)} records.")


    def _get_or_create_parent(self,record, cache):
        """
        Generates a hierarchy of resource entities.
        """
        provider = record.provider
        res_id = record.resource_id

        if provider == "aws":
            acc_id = record.account_id
            if acc_id not in cache:
                cache[acc_id] = self._upsert_single_parent(acc_id, provider, acc_id, "aws_account", None)
            return cache[acc_id]

        elif provider == "azure":
            parts = res_id.split("/")
            # Parse Subscription and ResourceGroup
            if len(parts) > 4 and parts[1].lower() == 'subscriptions':
                sub_id = parts[2]
                sub_ext_id = f"/subscriptions/{sub_id}"
                
                # Get Subscription
                if sub_ext_id not in cache:
                    cache[sub_ext_id] = self._upsert_single_parent(sub_ext_id, provider, sub_id, "subscription", None)
                sub_db_id = cache[sub_ext_id]

                # Get ResourceGroup
                if parts[3].lower() == 'resourcegroups':
                    rg_name = parts[4]
                    rg_ext_id = f"/subscriptions/{sub_id}/resourceGroups/{rg_name}"
                    if rg_ext_id not in cache:
                        cache[rg_ext_id] = self._upsert_single_parent(rg_ext_id, provider, rg_name, "resource_group", sub_db_id)
                    return cache[rg_ext_id]
                
                return sub_db_id

            # Fallback 
            fallback_sub_id = record.account_id
            fallback_ext = fallback_sub_id if fallback_sub_id.startswith('/') else f"/subscriptions/{fallback_sub_id}"
            if fallback_ext not in cache:
                cache[fallback_ext] = self._upsert_single_parent(fallback_ext, provider, record.account_id, "subscription", None)
            return cache[fallback_ext]

        return None

    def _upsert_single_parent(self, ext_id, provider, name, res_type, parent_id):
        query = """
            INSERT INTO Entities (ExternalId, ProviderName, ResourceName, ResourceType, ParentId)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ExternalId) DO UPDATE SET ParentId = EXCLUDED.ParentId
            RETURNING Id;
        """
        self.cursor.execute(query, (ext_id, provider, name, res_type, parent_id))
        return self.cursor.fetchone()[0]

    def _resolve_entities_bulk(self, records) -> dict:
        """
        Finds/Creates/Updates all entities
        """

        unique_entities = { record.resource_id: record for record in records }

        parent_cache = {}
        entity_values = []
        
        # JSONB concatenation based on https://www.postgresql.org/docs/9.5/functions-json.html
        # Coalesce to prevent NULL values
        # Some resources aren't updated by MetricsProcessor, but CostExports don't necessarily see all existing tags.
        insert_query = """
            INSERT INTO Entities (ExternalId, ProviderName, ResourceName, ResourceType,ParentId, Tags)
            VALUES %s
            ON CONFLICT (ExternalId) DO UPDATE 
            SET 
                Tags = COALESCE(Entities.Tags, '{}'::jsonb) || COALESCE(EXCLUDED.Tags, '{}'::jsonb)
            RETURNING Id, ExternalId
        """

        for rec in unique_entities.values():
            parent_id = self._get_or_create_parent(rec, parent_cache)
            entity_values.append((
                rec.resource_id,
                rec.provider,
                rec.resource_name,
                rec.resource_type,
                parent_id,
                json.dumps(rec.tags) if rec.tags else "{}"
            ))
        
        
        results = execute_values(self.cursor, insert_query, entity_values, fetch=True)
        
        # Return a dictionary for external ID and EntityID
        return {row[1]: row[0] for row in results}


    def _insert_costs_bulk(self, cost_values: list):
        """
        """
        query = """
            INSERT INTO Costs (
                EntityId, BilledCost, BillingCurrency, ChargePeriodStart, ChargePeriodEnd, 
                ServiceCategory, ServiceName, SkuPriceId
            ) VALUES %s
            ON CONFLICT (EntityId, ChargePeriodStart, ServiceName, SkuPriceId) 
            DO UPDATE SET 
                BilledCost = EXCLUDED.BilledCost,
                ChargePeriodEnd = EXCLUDED.ChargePeriodEnd
        """
        execute_values(self.cursor, query, cost_values)