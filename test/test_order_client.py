import socket, json

MESSAGE_DELIMITER = b'*'

order = {
    "symbol": "AAPL",
    "side": "BUY",
    "qty": 10,
    "price": 173.20
}

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect(("127.0.0.1", 9999))
    msg = json.dumps(order).encode('utf-8') + MESSAGE_DELIMITER
    s.sendall(msg)