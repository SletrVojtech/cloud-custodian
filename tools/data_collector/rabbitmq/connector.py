import pika
import logging
import os

log = logging.getLogger("rabbitmq_client")

class RabbitMQClient:
    """
    Based on:
    https://github.com/pazfelipe/python-rabbitmq
    (Felipe Paz, 2024)
    with added methods for using the 'with' clause
    """
    def __init__(self):
        self.user = os.getenv('RABBITMQ_USER', 'user')
        self.password = os.getenv('RABBITMQ_PASSWORD', 'password')
        self.host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.connection = None
        self.channel = None

    def connect(self):
        if self.connection and not self.connection.is_open:
            return
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(host=self.host,
                                                port=self.port,
                                                credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        log.debug("Connected to RabbitMQ")


    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        log.debug("Connection to RabbitMQ closed")
    
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def consume(self, queue_name, callback):
        if not self.channel:
            raise Exception("RabbitMQ Connection is not established.")
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        self.channel.start_consuming()

    def publish(self, queue_name, message):
        if not self.channel:
            raise Exception("Rabbit MQ Connection is not established.")
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_publish(exchange='',
                                   routing_key=queue_name,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))
        log.debug(f"Sent message to queue {queue_name}")