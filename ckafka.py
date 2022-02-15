from confluent_kafka import Consumer , Producer
import cv2 
from string import Template
import numpy as np 



"""
kafka consumer 
yeild need test (Feb. 15)

need :
json consumer
""" 
class cKafka_Consumer():
    def __init__(self, server_ip ,port ,topics):
        self.server_ip = server_ip
        self.port = port
        self.topics = topics
        self.group_id =0
        self.consumer = self.setting()
    
    def setting(self):
        kafkaserver = Template("${serveraddr}:${port}")
        server = kafkaserver.substitute(serveraddr=self.server_ip , port=self.port)
        consumer = Consumer({
                'bootstrap.servers': server ,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest'
            })

        consumer.subscribe(self.topics)
    
        return consumer
    
    """
    using :
    let var=func()
    for nvar in func:
        print nvar <- message
    """
    def image_Consumer(self) :
        # receive image using cv2
        while True:
            msg = self.consumer.poll(1)
            
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            # return image in while loop
            image_bytes = msg.value()
            yield cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)
        
    def test_Consumer(self):
        while True:
            msg = self.consumer.poll(1)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            yield msg.value().decode('utf-8')

    def close(self):
        self.consumer.close()
        
        
"""
kafka producer 

need :
image producer
test producer
json producer 
"""
class cKafka_Producer():
    def __init__(self, server_ip ,port ,topics):
        self.server_ip = server_ip
        self.port = port
        self.topic = topics
        self.producer = self.setting()

    def setting(self):
        kafkaserver = Template("${serveraddr}:${port}")
        server = kafkaserver.substitute(
            serveraddr=self.server_ip, port=self.port)
        producer = Producer({'bootstrap.servers': server})

        return producer
    
    def flush(self):
        self.producer.flush()
    
    """ 
    Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
    """
    def delivery_report(self,err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))
    
    def image_Producer(self , message , topic=None):
         # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)
        
        if self.topic is None:
            ptopic = topic
        else:
            ptopic = self.topic
        
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        self.producer.produce(ptopic, message.to_bytes(), callback=self.delivery_report)

    def test_Producer(self, message ,topic=None):
        self.producer.poll(0)

        if self.topic is None:
            ptopic = topic
        else:
            ptopic = self.topic

        self.producer.produce(ptopic, message.encode('utf-8'), callback=self.delivery_report)

    
    