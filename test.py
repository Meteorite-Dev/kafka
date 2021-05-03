from kafka import KafkaProducer, producer
from kafka.errors import KafkaError
import cv2
import os
from multiprocessing import Pool
import time
import argparse
from string import Template
import random
import csv

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

def writecsv(data):
    with open(path,'a+',newline="\n", encoding='utf-8') as f:
        csv_write = csv.writer(f)
        data_raw = data
        csv_write.writerow(data_raw)

if __name__ == "__main__":
    args = get_args()
    path = "test.csv"
    write_data=(args.thread,args.topicperthread)
    writecsv(write_data)