from multiprocessing.dummy import Pool
import threading
import time
import signal
from multiprocessing import Process

a = [(1, 2), (3, 4)]
for x1, x2 in a:
    print(x1, x2)