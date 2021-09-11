from confluent_kafka  import KafkaConsumer, consumer
import cv2
import numpy as np
from multiprocessing import Pool
import concurrent.futures
from string import Template
import argparse
import time
# To consume latest messages and auto-commit offsets
topic = 'topic1'
topic_tempstr = Template("topic${thread}_${serial}")
kafkaserver = Template("${serveraddr}:9092")

def setting_kafka(server):
    kafkastr = kafkaserver.substitute(serveraddr=server)
    consumer = KafkaConsumer(
            bootstrap_servers=[kafkastr]
        )
    return consumer 

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--width", help="input width", default=1280, type=int)
    parser.add_argument("--height", help="input height", default=720, type=int)
    parser.add_argument("--fps", help="video frame rate", default=30, type=int)
    parser.add_argument("--thread", help="number of threads", default=4, type=int)
    parser.add_argument("--sendframe", help="frame to send", default=300, type=int)
    parser.add_argument("--dropframe", help="frame to drop", default=30, type=int)
    parser.add_argument(
        "--camtest", help="use webcam to measure latency", action="store_true"
    )
    parser.add_argument("--random", help="to shuffle video list", action="store_true")
    parser.add_argument(
        "--topicperthread",
        help="the number of topic in thread to handle",
        default=1,
        type=int,
    )
    args = parser.parse_args()
    return args

def consume_video(tp):
    serial = tp[0]
    topicname = tp[1]
    consumer = setting_kafka("server")
    consumer.subscribe(topicname)
    print("%s connected !" % (topicname))
    timelist = []

    for msg in consumer:
        print(msg.offset)
        # img_byte = msg.value
        # frame = cv2.imdecode(np.frombuffer(img_byte,np.uint8),-1)
        # # print(frame.shape)
        # cv2.imshow('consumer', frame)
        # if cv2.waitKey(1) & 0xFF == ord('q'):
        #     break
        now_time = round(time.time() * 1000)
        send_time = msg.timestamp
        timelist.append((now_time - send_time) / 2)

    mean_intervaltime = sum(timelist) / len(timelist)
    print("%s stop !" % (topicname))
    return mean_intervaltime

def multithread_consume(args):
    topicstr = []
    for i in range(1, args.thread + 1):
        for j in range(1, args.topicperthread + 1):
            topicstr.append(topic_tempstr.substitute(thread=str(i), serial=str(j)))

    multithread_args = []
    for i in range(args.thread * args.topicperthread):
        multithread_args.append(
            (int((i / args.topicperthread) + 1), topicstr[i])
        )
        print(multithread_args[i])
    
    # pool = Pool(args.thread)
    # data = pool.map(consume_video, multithread_args)
    # pool.close()
    # pool.join()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.thread) as executor:
        data = executor.map(consume_video, multithread_args)
    print("----------------------------------------------")
    print("final mean time : ", sum(data) / len(data))
    pass

if __name__ == "__main__":
    args = get_args()
    multithread_consume(args)
    # measure(args)
