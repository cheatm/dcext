from threading import Thread, Timer
from queue import Queue, Empty
import logging


class Stream(object):

    def __iter__(self):
        yield
    

MESSAGE = "msg"
LOG = "log"


class Publisher(object):

    def pub(self, content):
        pass

    def log(self, content):
        pass


class Core(object):

    def __init__(self, stream, publisher):
        assert isinstance(stream, Stream)
        assert isinstance(publisher, Publisher)
        self.stream = stream
        self.publisher = publisher
        self.queue = Queue()
        self.stream_thread = None
        self.publish_thread = None
        self._running = False
        self.processed = 0
        self.timer = None
    
    def schdeule_log(self):
        if self._running:
            logging.warning("processed | %s", self.processed)
            timer = Timer(60, self.schdeule_log)
            timer.start()
            timer.join(0)
            self.timer = timer

    def start(self, wait=True):
        self._running = True
        self.publish_thread = Thread(target=self.handle)
        self.stream_thread = Thread(target=self.streaming)
        self.publish_thread.start()
        self.stream_thread.start()
        self.schdeule_log()
        if wait:
            try:
                self.join()
            except KeyboardInterrupt:
                self.timer.cancel()
    
    def stop(self, timeout=None):
        self._running = False
        self.join(timeout)
    
    def join(self, timeout=None):
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout)
        if self.publish_thread and self.publish_thread.is_alive():
            self.publish_thread.join(timeout)

    def streaming(self):
        try:
            for msg in self.stream:
                self.queue.put(msg)
                if not self._running:
                    break
        except Exception as e:
            logging.error("Streaming | %s", e)
            self._running = False

    def handle(self):
        while self._running or self.queue.qsize():
            try:
                _type, _data = self.queue.get(timeout=1)
            except Empty:
                continue
            except Exception as e:
                continue
            else:
                if _type == MESSAGE:
                    self.publish(_data)
                elif _type == LOG:
                    self.log(_data)
                self.processed += 1
    
    def publish(self, msg):
        self.publisher.pub(msg)
    
    def log(self, msg):
        self.publisher.log(msg)

