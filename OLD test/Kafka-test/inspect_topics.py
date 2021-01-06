from error import TopicError
from kafka import KafkaClient
from kafka.errors import TopicAlreadyExistsError, TopicAuthorizationFailedError

Compare = lambda M1,M2 : True if M1==M2 else False
Match = lambda C1,list : C1 in list

client = KafkaClient(bootstrap_servers='localhost:9092')
   
future = client.cluster.request_update()
client.poll(future=future)

metadata = client.cluster
topics = list(metadata.topics())
print(topics)

try:
    new_topic = input("Topic name  : ")
    topic_exist = Match(new_topic, topics)
    print(topic_exist)
    if not topic_exist:
        raise TopicAlreadyExistsError    
except TopicAlreadyExistsError as err :
    print("TopicAlreadyExistError : The {0} Topic is existed." .format(new_topic))

    
# print(type(topics))
# print(metadata.topics())

