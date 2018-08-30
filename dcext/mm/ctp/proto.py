from dcext.proto.jqbar_pb2 import BAR, BAR_REQ, BAR_RESP, Bar, BarReq, BarResp, JQMessage


MSG_TYPE = {
    BAR: Bar,
    BAR_REQ: BarReq,
    BAR_RESP: BarResp,
}


def parse_msg(msg):
    jq = JQMessage()
    jq.ParseFromString(msg)
    cls = MSG_TYPE[jq.type]
    _msg = cls()
    _msg.ParseFromString(jq.body)
    return _msg


def make_msg(msg_type, **kwargs):
    cls = MSG_TYPE[msg_type]
    msg = cls()
    for key, value in kwargs.items():
        setattr(msg, key, value)
    return msg


def make_jq_str(msg_type, **kwargs):
    body = make_msg(msg_type, **kwargs)
    jq = JQMessage()
    jq.type = msg_type
    _b = body.SerializeToString()
    print(_b)
    jq.body = _b
    return jq.SerializeToString()


def make_bar_req(symbol, start_time=200000, end_time=160000, trade_date=0):
    msg = make_msg(BAR_REQ, symbol=symbol, start_time=start_time, end_time=end_time, trade_date=trade_date)
    return msg.SerializeToString()
