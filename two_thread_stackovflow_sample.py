import threading
from queue import Queue
import time
import random

run = True


def clock(interval):
    while run:
        print("The time is %s" % time.ctime())
        time.sleep(interval)


def process(batch: list):
    print('Process batch: ', batch)
    time.sleep(3)


def worker(queue: Queue):
    batch = queue.get()
    while batch:
        process(batch)
        batch = queue.get()


queue = Queue()
t = threading.Thread(target=worker, args=(queue,))
t.daemon = True
t.start()
for i in range(10):
    L = [random.randint(0, 100) for _ in range(4)]
    print('Putting: ', L)
    queue.put(L)

print('Кладем пустой список')
queue.put([])
print('Все положили, ожидаем')
t.join()