import cv2

cap = cv2.VideoCapture(0)

while (True):
    _, frame = cap.read()
    print(frame.shape)
    cv2.imshow("camCapture", frame)
    if cv2.waitKey(1) == 27:
        cv2.destroyAllWindows()
        cap.release()
        break