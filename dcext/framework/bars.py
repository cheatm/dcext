from datetime import datetime, timedelta


FLOAT = 0.0
INT = 0


NEW = 0
UPD = 1
OLD = 2


class BaseBar(object):

    def on_tick(self, time, price , volume):
        raise NotImplementedError()

    def to_dict(self):
        raise NotImplementedError()
    
    def update(self, price, volume):
        raise NotImplementedError()


class Bar(BaseBar):

    def __init__(self, datetime, open=FLOAT, high=FLOAT, low=FLOAT, close=FLOAT, volume=INT):
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
    
    def on_tick(self, time, price , volume):
        raise NotImplementedError()
    
    def to_dict(self):
        return {
            "datetime": self.datetime,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }
    
    def update(self, price, volume):
        dct = {}
        if volume > self.volume:
            self.volume = volume
            dct["volume"] = volume
        if price == self.close:
            return dct
        else:
            dct["close"] = price
            self.close = price
            if price > self.high:
                self.high = price
                dct["high"] = price
            elif price < self.low:
                self.low = price
                dct["low"] = price
            return dct


class MinuteDelta(object):

    DELTA = 1

    def __init__(self):
        self.delta = timedelta(minutes=self.DELTA)
    
    def standart_time(self, time):
        minutes = time.minute - time.minute % self.DELTA
        return time.replace(minute=minutes, second=0, microsecond=0)


class Bar1M(Bar, MinuteDelta):

    DELTA = 1

    def __init__(self, datetime, open=FLOAT, high=FLOAT, low=FLOAT, close=FLOAT, volume=INT):
        MinuteDelta.__init__(self)
        super(Bar1M, self).__init__(self.standart_time(datetime), open, high, low, close, volume)

    def on_tick(self, time, price, volume=INT):
        if time - self.datetime >= self.delta:
            self.__init__(self.standart_time(time), price, price, price, price, volume)
            return NEW, self.to_dict()
        else:
            changed = self.update(price, volume if volume else self.volume)
            if changed:
                changed["datetime"] = self.datetime
                return UPD, changed 
            else:
                return OLD, None


class VBar(object):

    def __init__(self, datetime, open=FLOAT, high=FLOAT, low=FLOAT, close=FLOAT, init_volume=INT, last_volume=INT):
        self.datetime = datetime
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.init_volume = init_volume
        self.last_volume = last_volume
    
    @property
    def volume(self):
        return self.last_volume - self.init_volume
    
    def to_dict(self):
        return {
            "datetime": self.datetime,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "init_volume": self.init_volume,
            "last_volume": self.last_volume
        }

    def update(self, price, volume):
        dct = {}
        if volume > self.last_volume:
            self.last_volume == volume
            dct["last_volume"] = volume
            dct["volume"] = self.volume
        
        if price == self.close:
            return dct
        else:
            dct["close"] = price
            self.close = price
            if price > self.high:
                self.high = price
                dct["high"] = price
            elif price < self.low:
                self.low = price
                dct["low"] = price
            return dct


class VBar1M(VBar, MinuteDelta):

    DELTA = 1

    def __init__(self, datetime, open=FLOAT, high=FLOAT, low=FLOAT, close=FLOAT, init_volume=INT, last_volume=INT):
        MinuteDelta.__init__(self)
        super(VBar1M, self).__init__(self.standart_time(datetime), open, high, low, close, init_volume, last_volume)

    def on_tick(self, time, price, volume=INT):
        if time - self.datetime >= self.delta:
            self.__init__(self.standart_time(time), price, price, price, price, self.last_volume, volume)
            return NEW, self.to_dict()
        else:
            changed = self.update(price, volume)
            if changed:
                changed["datetime"] = self.datetime
                return UPD, changed
            else:
                return OLD, None
        

class Handler(object):

    def handle(self, data):
        pass


class Publisher(object):

    def __iter__(self):
        raise NotImplementedError()

    def stop(self):
        pass


from threading import Thread
from queue import Queue, Empty
import logging


class CoreEngine(object):

    def __init__(self):
        self.handlers = {}
        self.publishers = {}
        self.queue = Queue()
        self._running = False
        self.threads = {} 
    
    def register_publisher(self, name, publisher):
        assert isinstance(publisher, Publisher)
        self.publishers[name] = publisher
    
    def register_handler(self, name, handler):
        assert isinstance(handler, Handler)
        self.handlers[name] = handler
    
    def run_publisher(self, name):
        publisher = self.publishers[name]
        iterable = publisher.__iter__()
        while self._running:
            try:
                item = next(iterable)
            except StopIteration:
                break
            except Exception as e:
                logging.error("publisher next item error | %s | %s", name, e)
            else:
                self.queue.put(item)
        publisher.stop()
    
    def start(self):
        self._running = True
        for key in self.publishers.keys():
            self.start_publisher(key)
        self.main_loop()
    
    def main_loop(self):
        while self._running:
            try:
                tag, value = self.queue.get(timeout=1)
            except Empty:
                continue
            except KeyboardInterrupt:
                self._running = False
                continue
            except Exception as e:
                logging.error("excepting message from queue | %s", e)
                continue
            try:
                handler = self.handlers[tag]
                handler.handle(value)
            except KeyboardInterrupt:
                self._running = False
                continue
            except Exception as e:
                logging.error("handle message | %s | %s | %s", tag, value, e)


    def start_publisher(self, name):
        if name in self.threads:
            thread = self.threads[name]
            if thread.is_alive():
                logging.warning("Publish thread | %s | already start")
                return
            else:
                logging.warning("Publish thread | %s | not alive, create new thread")
        
        thread = Thread(target=self.run_publisher, daemon=True, args=(name, ))
        self.threads[name] = thread
        thread.start()
