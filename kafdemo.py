# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
import argparse
import glob
import multiprocessing as mp
import os
import time
import cv2
import tqdm

from detectron2.data.detection_utils import read_image
from detectron2.utils.logger import setup_logger

from predictor import VisualizationDemo
from adet.config import get_cfg

from ckafka import cKafka_Consumer,cKafka_Producer

# constants
WINDOW_NAME = "COCO detections"


def setup_cfg(args):
    # load config from file and command-line arguments
    cfg = get_cfg()
    cfg.merge_from_file(args.config_file)
    cfg.merge_from_list(args.opts)
    # Set score_threshold for builtin models
    cfg.MODEL.RETINANET.SCORE_THRESH_TEST = args.confidence_threshold
    cfg.MODEL.ROI_HEADS.SCORE_THRESH_TEST = args.confidence_threshold
    cfg.MODEL.FCOS.INFERENCE_TH_TEST = args.confidence_threshold
    cfg.MODEL.MEInst.INFERENCE_TH_TEST = args.confidence_threshold
    cfg.MODEL.PANOPTIC_FPN.COMBINE.INSTANCES_CONFIDENCE_THRESH = args.confidence_threshold
    cfg.freeze()
    return cfg


def get_parser():
    parser = argparse.ArgumentParser(description="Detectron2 Demo")
    parser.add_argument(
        "--config-file",
        default="configs/BlendMask/DLA_34_syncbn_4x.yaml",
        metavar="FILE",
        help="path to config file",
    )
    parser.add_argument("--intopic" ,nargs="+" ,help="--intopic <topic> ,using kafka input image." ,type=str ,required=True)
    parser.add_argument("--ip",nargs="+",help="kafka server ip." ,default="localhost" ,type=str)
    parser.add_argument("--port" ,nargs="+" ,help="kafka server port." ,default="9094" ,type=str)
    parser.add_argument("--outtopic", help="--outtopic <topic> ,kafka transfer to <topic>" ,type=str ,required=True)
    parser.add_argument(
        "--imshow",
        help="A file or directory to save output visualizations. "
        "If not given, will show output in an OpenCV window.",
    )

    parser.add_argument(
        "--confidence-threshold",
        type=float,
        default=0.3,
        help="Minimum score for instance predictions to be shown",
    )
    parser.add_argument(
        "--opts",
        help="Modify config options using the command-line 'KEY VALUE' pairs",
        default=[],
        nargs=argparse.REMAINDER,
    )
    return parser


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    args = get_parser().parse_args()
    logger = setup_logger()
    logger.info("Arguments: " + str(args))

    cfg = setup_cfg(args)

    demo = VisualizationDemo(cfg)

    # print(args.ip[0] , args.port)
 
    consumer = cKafka_Consumer(server_ip=args.ip[0] ,port=args.port[0] ,topics=args.intopic)
    producer = cKafka_Producer(server_ip=args.ip[0], port=args.port[0], topics=args.outtopic[0])
    print("recvtime,inference_time,instances_time,alltime")
    """
    producer not yet
    """
    for mes in consumer.image_Consumer(t=True):
        if mes is None:
            # print(type(mes))
            continue
        start_time = time.time()
        st = mes[1][1]
        rt = mes[2]
        # print(mes)

        # print("receive time : " , (rt-st))
        print((rt-st),",",end="")
        image = mes[0]
        # print("mes type" ,type(mes))
        predictions, visualized_output = demo.run_on_image(image)
        print((time.time()-start_time),",",end="")

        # logger.info(
        #     "iamge : detected {} instances in {:.2f}s"
        #         .format(len(predictions["instances"]), 
        #         time.time() - start_time
        #     )
        # )
        # print("All time : " , time.time() - int(st) )
        print((time.time()-int(st)))
        
        if args.imshow:
            pass
            cv2.imshow(WINDOW_NAME, visualized_output.get_image()[:, :, ::-1])
            if cv2.waitKey(0) == 27:
                break  # esc to quit
        else:
            # print("send image to {}" .format(args.outtopic))
            producer.image_Producer(message= visualized_output.get_image()[:, :, ::-1] , topic=args.outtopic ,t=True)