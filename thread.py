import threading
from kafbasic import Producer

producer = Producer()
producer.producer_set(host=['localhost:9092'] ,topics="testTopic")

def prod(thrnum) :
    for i in range(100) :
        mes = "test" +" : "+str(thrnum)
        res = producer.tsend(topics="thread" + str(thrnum) , message=mes , react=True)
        print(res)
threads = []
for i in range(5) :
    threads.append( threading.Thread(target=prod , args=(i,)) )

    threads[i].start()
    print(i)

print("main")

print(threading.active_count())

# for i in range(5):
#     threads[i].join()


print("done.")
