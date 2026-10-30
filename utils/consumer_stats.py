"""
utils/consumer_stats.py
Each Stage 1 / Stage 2 process reports which partitions Kafka assigned to it.
The report goes to taxi.metrics as {"type": "consumer_stats", ...}; the final
consumer counts the reports, so the consumer columns in the results CSV come
from the processes that actually ran, not from a command-line value.
"""
import os
import socket

from kafka import ConsumerRebalanceListener


class AssignmentReporter(ConsumerRebalanceListener):
    def __init__(self, producer, metrics_topic: str, stage: str):
        self.producer = producer
        self.metrics_topic = metrics_topic
        self.stage = stage
        self.instance = f"{socket.gethostname()}-{os.getpid()}"

    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        partitions = sorted(tp.partition for tp in assigned)
        self.producer.send(self.metrics_topic, value={
            "type": "consumer_stats",
            "stage": self.stage,
            "instance": self.instance,
            "partitions": partitions,
        })
        self.producer.flush()
