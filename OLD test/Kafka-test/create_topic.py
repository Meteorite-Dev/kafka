from kafka import KafkaClient
from kafka.errors import TopicAlreadyExistsError

from kafka.admin import KafkaAdminClient, NewTopic


Compare = lambda M1,M2 : True if M1==M2 else False
Match = lambda C1,Mlist : C1 in Mlist


client = KafkaClient(bootstrap_servers='localhost:9092')
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)

try:
    future = client.cluster.request_update()
    client.poll(future=future)

    metadata = client.cluster
    topics = list(metadata.topics())
    print(topics)
    
    new_topic = input("Topic name  : ")
    topic_exist = Match(new_topic, topics)
    print(topic_exist)
    if topic_exist:
        raise TopicAlreadyExistsError
except TopicAlreadyExistsError as err:
    print("TopicAlreadyExistError : The {0} Topic is existed." .format(new_topic))



topic_list = []
# topic_list.append(NewTopic(name=new_topic,num_partitions=1, replication_factor=1))
# admin_client.create_topics(new_topics=topic_list, validate_only=False)
topics = admin_client.list_topics()
print(topics)
