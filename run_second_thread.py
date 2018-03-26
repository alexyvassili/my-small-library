from queue import Queue
import threading
from time import sleep

def worker(batch_queue):
    i = batch_queue.get()
    while i is not None:
        print('WORKER: I am worked on work {}'.format(i))
        sleep(3)
        i = batch_queue.get()


def upload(i, batch_queue):
    print('This is {} work'.format(i))
    batch_queue.put(i)
    sleep(2)


if __name__ == "__main__":
    # Создаем очередь для батчей загрузки в бд, ее будет разбирать отдельный тред
    batch_queue = Queue()
    upload_thread = threading.Thread(target=worker, args=(batch_queue,))
    upload_thread.daemon = True
    print('Start upload to db thread...')
    upload_thread.start()

    for i in range(10):
        upload(i, batch_queue)

    print('Кладем пустой batch, чтобы воркер понял, что пора завершаться')
    batch_queue.put(None)
    upload_thread.join()
