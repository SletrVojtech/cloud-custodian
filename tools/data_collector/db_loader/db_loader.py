import logging
from psycopg2.extras import execute_values
from tools.data_collector.rabbitmq.message import IngestionMessage
from tools.data_collector.db_loader.metrics_processor import MetricsProcessor


log = logging.getLogger('DB_loader')

class DBLoader:
    def __init__(self, db_conn, mq_channel):
        self.db = db_conn
        self.mq = mq_channel

    def start_consuming(self, queue_name="metrics_ingestion"):
        """Main consumer loop"""
        self.mq.basic_qos(prefetch_count=50)
        self.mq.basic_consume(
            queue=queue_name, 
            on_message_callback=self.handle_message, 
            auto_ack=False
        )

        self.mq.start_consuming()

    def handle_message(self, ch, method, properties, body):
        try:

            envelope = IngestionMessage.model_validate_json(body)
            
            # Switch upon source module
            if envelope.source_module == "custodian":
                processor = MetricsProcessor(self.db)
                processor.process(envelope)
            else:
                log.warning(f"Unsupported module: {envelope.source_module}")

            # Message has been saved into the DB, remove from RabbitMQ queue.
            self.db.commit() 
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            self.db.rollback()
            log.error(f"Exception during message processing: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)