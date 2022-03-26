import json

from kafka import KafkaConsumer
from kafka.errors import kafka_errors


class KafkaConsumerClient:
    def __init__(self):
        self.client = KafkaConsumer(
            'zzlion',
            bootstrap_servers='192.168.9.128:9092',
            group_id='test',
            auto_offset_reset='earliest'
        )

    def demo(self):
        for message in self.client:
            print(
                f"receive, key: {json.loads(message.key.decode())}, "
                f"value: {json.loads(message.value.decode())}"
            )


if __name__ == '__main__':
    KafkaConsumerClient().demo()
