from kafbasic import Producer
import cv2 as cv

def show(image):
    center = (image.shape[0]/4, image.shape[1]/4)
    cv.namedWindow("image", flags=cv.WINDOW_NORMAL)
    cv.resizeWindow("image", (int(center[1]), int(center[0])))
    cv.imshow("image", image)
    cv.waitKey()
    cv.destroyAllWindows()

host = ['localhost:9092']
topics="testTopic"
ser = lambda v:v.tobytes()
producer = Producer()
producer.producer_set(host=host, serializer=ser)


image = cv.imread("test.png" ,  cv.IMREAD_COLOR)
# show(image)
center = (image.shape[0]/4, image.shape[1]/4)
image = cv.resize(image,  (int(center[1]), int(center[0])))

fur = producer.send(topics=topics , message=image , react=True)
print(fur)
