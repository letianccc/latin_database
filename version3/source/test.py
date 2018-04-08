from multiprocessing.dummy import Pool
import threading
import time
import signal
from multiprocessing import Process

class Value:
    value = [0]

    def plus(n):
        Value.value[0] += n

    def multiply(n):
        Value.value[0] *= n



class Plus(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.daemon = True

    def run(self):
        for i in range(1000):
            # time.sleep(1)
            Value.plus(2)
            print('plus', Value.value[0])

class Mult(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.daemon = True

    def run(self):
        for i in range(5):
            time.sleep(1)
            Value.multiply(2)
            print('mult', Value.value)



# # t = threading.Thread(target = f, args = (1, 2))
# t = T([1, 2], f)
# # t.daemon = True
# t.start()
# time.sleep(5)
# print('**')

# for i in range(5):
#     p = Plus()
#     m = Mult()
#     p.start()
#     m.start()
#     print(Value.value)

# p1 = Plus()
# p2 = Plus()
# # m = Mult()
# p1.start()
# p2.start()
# for i in range(5):
#     p = Plus()
#     p.start()
# m.start()
# time.sleep(100)
l = []
# if l:
#     print(True)
# else:
#     print(False)
for e in l:
    print('**')