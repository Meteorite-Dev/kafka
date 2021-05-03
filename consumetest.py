from kafka import KafkaConsumer, consumer
import cv2
import numpy as np
from multiprocessing import Pool
from string import Template
import argparse
import time
kafkaserver = Template("${serveraddr}:9092")
kafkastr = kafkaserver.substitute(serveraddr='server')
consumer = KafkaConsumer(
            bootstrap_servers=[kafkastr]
        )
consumer.subscribe("topic1_1")
for msg in consumer:
    print("receive")