import json
import time
from typing import Callable

import pika
import requests as requests
from decouple import config
import atexit

from pika.credentials import ExternalCredentials, PlainCredentials

from errors import ValidationError


class BankingService:
    channel = None
    connection = None
    url = config('ORDER_SERVICE_URL', default=False, cast=str)

    def __init__(self):
        self._connect()

    def _connect(self):
        # Connection parameters
        host = config('RABBITMQ_HOST', default=False, cast=str)
        username = config('RABBITMQ_USERNAME', default=False, cast=str)
        password = config('RABBITMQ_PASSWORD', default=False, cast=str)
        connection_params = pika.ConnectionParameters(
            host=host, credentials=PlainCredentials(username=username,
                                                    password=password))
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()

    def _listen_queue(self, queue_name: str, callback: Callable):
        # Declare a queue named 'checkout_queue'
        self.channel.queue_declare(queue=queue_name)

        # Specify the callback function to be called when a message is received
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=callback,
                                   auto_ack=True)
        print(' [*] Waiting for messages. To exit, press CTRL+C')
        self.channel.start_consuming()

    def _execute_banking(self, ch, method, properties, body):
        print(f" [x] Received: {body.decode()}")
        order_dict = json.loads(body)
        id = order_dict["id"]
        query = """mutation {{
                    changePaymentStatus(id:{0}, paymentStatus:"paid"){{
                    id
                    paymentStatus
                }}
            }}"""
        formatted_query = query.format(id)
       # time.sleep(1500)
        response = requests.post(self.url,
                                 data={'query': formatted_query})
        self.validate_errors(response)
        print('calculated')

    @staticmethod
    def validate_errors(response):
        if 'errors' in str(response.content):
            cleaned_json = json.loads(
                response.content.decode('utf-8').replace("/", "")
            )['errors']
            raise ValidationError(cleaned_json[0]['message'])

    def exit_handler(self):
        self.connection.close()

    def start(self):
        self._connect()
        self._listen_queue("banking_queue",
                           callback=self._execute_banking)


if __name__ == '__main__':
    checkout_service = BankingService()
    atexit.register(checkout_service.exit_handler)
    checkout_service.start()
