import duckdb
import logging
from datetime import datetime, timedelta
from tools.data_collector.cost_collector.message import CostPayload, CostBatchPayload
from tools.data_collector.rabbitmq.message import IngestionMessage


log = logging.getLogger("cost_loader")

class CostDataLoader:
    """
    Uses DUCKDB in-memory querying over downloaded files and batch uploads aggregated data to RabbitMQ.
    """
    def __init__(self, rmq_client, queue_name="data_ingestion"):
        self.rmq_client = rmq_client
        self.queue_name = queue_name
        self.con = duckdb.connect(database=':memory:')

    def process_and_publish(self, export_folder_pattern: str, batch_size: int = 1000, days_back: int = 7):
        log.info(f"Running DuckDB on files: {export_folder_pattern}")
        cutoff_date = (datetime.now(datetime.timezone.utc) - timedelta(days=days_back)).strftime('%Y-%m-%d 00:00:00')
        
        query = f"""
            SELECT 
                ProviderName,
                SubAccountId,
                RegionName,
                COALESCE(ResourceId, SubAccountId || ':general') AS resource_id,
                ResourceName,
                ResourceType,
                Tags,
                ServiceCategory,
                ServiceName,
                SkuPriceId,
                BillingCurrency,
                
                CAST(ChargePeriodStart AS TIMESTAMP) AS charge_period_start,
                MAX(CAST(ChargePeriodEnd AS TIMESTAMP)) AS charge_period_end,
                
                SUM(BilledCost) AS billed_cost
            FROM read_csv_auto('{export_folder_pattern}', header=True)
            WHERE BilledCost != 0 
            AND CAST(ChargePeriodStart AS TIMESTAMP) >= CAST('{cutoff_date}' AS TIMESTAMP)
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        """
        
        try:
            # Run the aggregation
            result_dicts = self.con.execute(query).fetchdf().to_dict('records')
            total_records = len(result_dicts)
            log.info(f"DuckDB aggregation completed, {total_records} unique records.")

            if not result_dicts:
                return

            # Parse by Pydantic
            records = []
            for row in result_dicts:
                
                records.append(CostPayload(**row))

            "Batch upload to RabbitMQ"
            for i in range(0, total_records, batch_size):
                batch_records = records[i:i + batch_size]
                
                payload = CostBatchPayload(
                    records=batch_records
                )

                message = IngestionMessage(
                    source_module="cost_export",
                    payload=payload.model_dump()
                )
                
                self.rmq_client.publish(
                    queue_name=self.queue_name, 
                    message=message.model_dump_json()
                )
            
            log.info(f"Sent {total_records} records to RabbitMQ in {total_records // batch_size + 1} batches.")
                
            return

        except Exception as e:
            log.error(f"Error during DuckDB aggregation: {e}", exc_info=True)
            return