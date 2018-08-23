import zmq
from dcext.framework.transmit import Publisher


def get_publish_sock(address):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(address)
    socket.bind
    return socket


def get_req_sock(address):
    context = zmq.Context()  
    socket = context.socket(zmq.REQ)
    socket.connect(address)
    return socket


class ZMQPublisher(Publisher):

    def __init__(self, socket):
        assert isinstance(socket, zmq.Socket)
        assert socket.type == zmq.PUB
        self.socket = socket

    @classmethod
    def from_addr(cls, addr):
        return cls(get_publish_sock(addr))

    def pub(self, content):
        # self.socket.send_string(content)
        self.socket.send_multipart([content])
