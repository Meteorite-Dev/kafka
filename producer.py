from kafka import KafkaProducer
from kafka.errors import KafkaError
import cv2
import os
from multiprocessing import Pool
import time
import argparse
from string import Template
import random
import csv

def setting_kafka(server, size=3145728):
    try:
        kafkastr = kafkaserver.substitute(serveraddr=server)
        producer = KafkaProducer(
            bootstrap_servers=[kafkastr], max_request_size=size,compression_type="lz4"
        )
        return producer
    except KafkaError as e:
        print(e)

def setting_capture(path):
    args = get_args()
    capture = cv2.VideoCapture(path)
    capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    capture.set(cv2.CAP_PROP_FRAME_WIDTH, args.width)
    capture.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)
    capture.set(cv2.CAP_PROP_FPS, args.fps)
    return capture

def writecsv(data):
    file = "producer.csv"
    with open(file,'a+',newline="\n", encoding='utf-8') as f:
        csv_write = csv.writer(f)
        data_raw = data
        csv_write.writerow(data_raw)

def publish_video(tp):
    file = tp[0]
    serial = tp[1]
    topicname = tp[2]
    args = get_args()
    producer = setting_kafka('server')
    capture = setting_capture(file)
    print("%s start !" % (topicname))
    timelist = []
    totalframe = args.sendframe + args.dropframe
    for i in range(totalframe):
        _, frame = capture.read()
        if i < args.dropframe:
            continue
        # encode frame to jpeg then tobytes
        data = cv2.imencode(".jpeg", frame)[1].tobytes()
        future = producer.send(topicname, value=data)
        metadata = future.get(timeout=1)
        now_time = round(time.time() * 1000)
        send_time = metadata.timestamp
        timelist.append((now_time - send_time) / 2)
    mean_intervaltime = sum(timelist) / len(timelist)
    print("%s stop !" % (topicname))
    return mean_intervaltime

def multithread_publish(args):
    video_dir = "/media/cluster/0EA405370EA40537/video/"
    video_list = []
    for fname in os.listdir(video_dir):
        video_list.append(os.path.join(video_dir, fname))
    if args.random:
        random.shuffle(video_list)
    topicstr = []
    for i in range(1, args.thread + 1):
        for j in range(1, args.topicperthread + 1):
            topicstr.append(topic_tempstr.substitute(thread=str(i), serial=str(j)))

    multithread_args = []
    for i in range(args.thread * args.topicperthread):
        multithread_args.append(
            (video_list[i], int((i / args.topicperthread) + 1), topicstr[i])
        )
        # print(multithread_args[i])

    pool = Pool(args.thread)
    data = pool.map(publish_video, multithread_args)
    pool.close()
    pool.join()
    print("----------------------------------------------")
    latency = sum(data) / len(data)
    print("final mean time : ", latency)
    write_data=(args.thread,args.topicperthread,latency)
    writecsv(write_data)
    # print(res)

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


if __name__ == "__main__":
    topic_tempstr = Template("topic${thread}_${serial}")
    kafkaserver = Template("${serveraddr}:9092")
    args = get_args()
    multithread_publish(args)
