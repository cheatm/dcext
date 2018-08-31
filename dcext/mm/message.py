from dcext.zeromq import subscribe, get_sub_sock
from dcext.framework.bars import Publisher
from dcext.proto.jzs_pb2 import MSG_MD_MARKETDATA_IND, Msg
from dcext.proto.md_pb2 import MarketDataInd, MD_STK_L1, MD_FUT_L2, MD_STK_L2, MD_FUT_L1


def get_proto(msg, cls):
    obj = cls()
    obj.ParseFromString(msg)
    return obj


def get_rsp(msg):
    _m = get_proto(msg, Msg)
    if _m.head.tid == MSG_MD_MARKETDATA_IND:
        return get_proto(_m.body, MarketDataInd)


def msg2quote(msg):
    return get_rsp(msg[4:])    



class QuotePublisher(Publisher):

    def __init__(self, addr):
        self.addr = addr
        self.sock = get_sub_sock(addr)
    
    def __iter__(self):
        while True:
            msg = self.sock.recv_multipart()[0]
            ind = msg2quote(msg)
            if ind:
                if ind.type in [MD_FUT_L1, MD_FUT_L2]:
                    quote = ind.fut
                else:
                    quote = ind.stk
                
                yield quote


def main():
    qp = QuotePublisher("tcp://127.0.0.1:10001")
    for quote in qp:
        print(quote)


if __name__ == '__main__':
    main()

