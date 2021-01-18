from kafka import KafkaProducer
from kafka import KafkaConsumer

class Producer():
    def __init__(self) :
        self.Topic_host = ['localhost:9092']
        self.Topic = "testTopic"
    
    def producer_set(self , host=None , serializer=None) :

        
        if host != None :
            self.Host = host
        
        if serializer == None:
            serializer = lambda v : v.encode('utf-8')

        self.producer = KafkaProducer(
            bootstrap_servers=self.Host, value_serializer=serializer, max_request_size=32000000)
    
    
    # def send(self, message, key=None , react=True) :
    #     if key ==None :
    #         if react:
    #             future = self.producer.send(self.Topic, value=message)
    #             result = future.get(timeout=10)
    #             return result
    #         else :
    #             self.producer.send(self.Topic , value= message  )

    #     elif key !=None:
    #         if react:
    #             future = self.producer.send(self.Topic, WeakValueDictionary=message , key=key)
    #             result = future.get(timeout=10)
    #             return result
    #         else :
    #             self.producer.send(self.Topic, value=message, key=key)
    #     else :
    #         print("error")
    
    def send(self ,topics, message, key=None, react=True):
        if key == None:
            if react:
                future = self.producer.send(topics, value=message)
                result = future.get(timeout=10)
                return result
            else:
                self.producer.send(topics, value=message)

        elif key != None:
            if react:
                future = self.producer.send(
                    topics, WeakValueDictionary=message, key=key)
                result = future.get(timeout=10)
                return result
            else:
                self.producer.send(topics, value=message, key=key)
        else:
            print("error")

class Consumer():
    def __init__(self ):
        self.Host = ['localhost:9092']
        self.Topic = "testTopic"
        self.waiting_ms =10000

    def consumer_set(self, host=None, topics=None , wait=False):
        if topics ==None:
            topics =self.Topic

        if host != None:
            self.Host = host
        
        if wait :
            self.consumer = KafkaConsumer( bootstrap_servers=self.Host, value_deserializer = lambda v : v.decode('utf-8'),  consumer_timeout_ms=self.waiting_ms)
        else :
            self.consumer = KafkaConsumer( bootstrap_servers=self.Host , value_deserializer = lambda v : v.decode('utf-8'))

        self.consumer.subscribe(topics)


    def listen(self , tp=None):
        if tp ==None:
            for msg in self.consumer:
                print(msg)
        else :
            tp = tp.lower()
            if tp=="ms":
                for msg in self.consumer:
                    print(msg.value)
            elif tp =="msk":
                for msg in self.consumer:
                    print("value :" ,msg.value)
                    print("key :" , msg.key)
