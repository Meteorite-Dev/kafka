from tkinter import N
from confluent_kafka import Consumer , Producer
import cv2 
from string import Template
import numpy as np 
import json
import time
from turbojpeg import TurboJPEG
import  quickle

"""
kafka consumer 
update time methood.
""" 
class cKafka_Consumer():
    def __init__(self, server_ip ,port ,topics):
        self.server_ip = server_ip
        self.port = port
        self.topics = topics
        self.group_id =0
        self.consumer = self.setting()
        self.jpeg = TurboJPEG()
    
    def setting(self):
        kafkaserver = Template("${serveraddr}:${port}")
        server = kafkaserver.substitute(serveraddr=self.server_ip , port=self.port)
        
        consumer = Consumer({
            'bootstrap.servers': server,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest'
        })
        
        consumer.subscribe(self.topics)
    
        return consumer

    # json deserializer from utf-8
    def json_deserializer(self ,jsinput):
        if jsinput is None :
            return None
        else:
            try:
               return json.loads(jsinput.decode('utf-8'))  
            except json.decoder.JSONDecodeError :
                print("Unable to decode: %s" %jsinput)
                return -1

    def mes_timestamp(self, mes):
        time = mes.timestamp()
        return time
    
    """
    using :
    let var=func()
    for nvar in func:
        print nvar <- message
    """
    def image_Consumer(self , t=False) :
        # receive image using cv2
        while True:
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            rtime = time.time()
            # print("msg:",type(msg))
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            
            # return image in while loop            
            image_bytes = msg.value()            
            # image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)
            image = self.jpeg.decode(np.frombuffer(image_bytes, np.uint8))
            if t :
                stime = self.mes_timestamp(msg)
                yield image , stime , rtime
            else :
                yield image 

    """
    json consumer
    """
    def Json_Consumer(self, t=False):
        while True:
            msg = self.consumer.poll(1)
            rtime = time.time()
            if msg is None:
               continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            
            # return json in while loop
            ori_js_mes = msg.value()
            json_mes = self.json_deserializer(ori_js_mes)
            
            if t:
                stime = self.mes_timestamp(msg)
                yield json_mes , stime , rtime
            else :
                yield json_mes
            
    def test_Consumer(self, t=False):
        while True:
            msg = self.consumer.poll(1)
            rtime = int(time.time())
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            if t :
                stime = self.mes_timestamp(mes=msg)
                yield msg.value().decode('utf-8') , stime , rtime 
            else:
                yield msg.value().decode('utf-8')
    
    def basic_Consumer(self , t=False) :
        while True:
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            rtime = time.time()
            # print("msg:",type(msg))
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue      
            msgBytes = msg.value()            
            # return bytes
            if t :
                stime = self.mes_timestamp(msg)
                yield msgBytes , stime , rtime
            else :
                yield msgBytes 
    
    def imgdic_Consumer(self ,t=False):
        """
        consumer for quickle object.
        transfer to image and dict 
        """
        while True :
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            rtime = time.time()

            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            upick = msg.value()
            upk_dict = quickle.loads(upick)
            # image = upk_dict['image']
            # data = upk_dict['data']            
            if t :
                stime = self.mes_timestamp(msg)
                yield upk_dict ,stime ,rtime
            else :
                yield upk_dict

    def quickle_to_imgdata(self , quickle_obj):
        image = quickle_obj['image']
        data = quickle_obj['data']
        yield image ,data 


    def close(self):
        self.consumer.close()
        
        
"""
kafka producer 
update time methood.
"""
class cKafka_Producer():
    def __init__(self, server_ip ,port ,topics):
        self.server_ip = server_ip
        self.port = port
        self.topic = topics
        self.producer = self.setting()
        self.jpeg = TurboJPEG()

    def setting(self):
        kafkaserver = Template("${serveraddr}:${port}")
        server = kafkaserver.substitute(
            serveraddr=self.server_ip, port=self.port)
        producer = Producer({'bootstrap.servers': server})

        return producer
    
    def flush(self):
        self.producer.flush()

    def prod(self , message ,key=None, topic_name=None , timestamp=False):
        if topic_name is None:
            topic_name = self.topic 
        if timestamp :
            self.producer.produce(
                topic=topic_name, key=key ,value=message, callback=self.delivery_report, timestamp=int(time.time()))
        else:
            
            self.producer.produce(topic=topic_name, key=key, value=message,
                                  callback=self.delivery_report)

    
    """ 
    Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). 
    """
    def delivery_report(self,err, msg):
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        # else:
        #     print('Message delivered to {} [{}]'.format(
        #         msg.topic(), msg.partition()))
    
    def json_serizilier(self , message):
        jsmes = json.dumps(message).encode('utf-8')
        return jsmes

    def image_Producer(self , message , key=None,topic=None, t=False):
         # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)
        
        if self.topic is None:
            ptopic = topic
        else:
            ptopic = self.topic
        
        # message = cv2.imencode('.jpg' ,message)[1]
        
        message = self.jpeg.encode(message)
        # tobytes or not tobytes? 
        # is a question
        # message = message.tobytes()

        self.prod(message=message,key=key , topic_name=ptopic , timestamp=t)

    def json_Producer(self, message, key=None, topic=None, t=False):
        self.producer.poll(0)

        if self.topic is None:
            ptopic = topic
        else:
            ptopic = self.topic

        message = self.json_serizilier(message=message)
        self.prod(message=message,key=key , topic_name=ptopic , timestamp=t)

    def test_Producer(self, message, key=None, topic=None, t=False):
        self.producer.poll(0)

        if self.topic is None:
            ptopic = topic
        else:
            ptopic = self.topic

        self.prod(message=message,key=key , topic_name=ptopic , timestamp=t)

    def imgdic_Producer(self , image, imdata , key=None, topic=None, t=False):
        self.producer.poll(0)
        mes_obj = {"image":image ,"data" : imdata}

        # dict to quickle
        pikobj = quickle.dumps(mes_obj)

        if topic is None :
            topic = self.topic
        
        self.prod(message=pikobj, key=key, topic_name=topic, timestamp=t)
