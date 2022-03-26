import json
import traceback

from kafka import KafkaProducer
from kafka.errors import kafka_errors


class KafkaProducerClient:
    def __init__(self):
        self.client = KafkaProducer(
            bootstrap_servers=['192.168.9.128:9092'],
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode()
        )

    def demo(self):
        for i in range(4, 10):
            future = self.client.send(
                'zzlion',
                key='count',
                value=str(i),
                partition=0
            )
            print(f'send {i}')
            try:
                future.get(timeout=10)
            except kafka_errors:
                traceback.format_exc()


if __name__ == '__main__':
    KafkaProducerClient().demo()
