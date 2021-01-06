from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaClient

class Kafka_Normal_test():
    def __init__(self , server_ip , id):
        self.client_id = id
        self.Host_ip = server_ip 
        self.admin_client = KafkaAdminClient( bootstrap_servers=server_ip,client_id=self.client_id)
        self.Client = KafkaClient(bootstrap_servers=server_ip)

    def Topic_Check(self , Topic_name) :
        Match = lambda C1, list: C1 in list
        new_topic = Topic_name

        try:
            future = self.Client.cluster.request_update()
            self.Client.poll(future=future)

            metadata = self.Client.cluster
            topics = list(metadata.topics())
            print(topics)

            topic_exist = Match(new_topic, topics)
            print(topic_exist)
            if topic_exist:
                print("exist.")
                raise TopicAlreadyExistsError
        except TopicAlreadyExistsError :
            print("TopicAlreadyExistError : The {0} Topic is existed." .format(new_topic))
            # 強制中斷
            assert False 


    def Topic_create(self , New_Topic):
        self.Topic_Check(Topic_name = New_Topic)
        topic_list = []
        topic_list.append(NewTopic(name=New_Topic,num_partitions=0, replication_factor=1))
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        topics = self.admin_client.list_topics()
        print(topics)

    def Inspect_Topic(self):
        future = self.Client.cluster.request_update()
        self.Client.poll(future=future)

        metadata = self.Client.cluster
        topics = list(metadata.topics())
        print(topics)

if __name__ == '__main__':
    K = Kafka_Normal_test(server_ip="localhost:9092" , id="test")
    K.Inspect_Topic()
    K.Topic_create(New_Topic="example_topic")
