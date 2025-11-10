import socket
import json
import threading

MESSAGE_DELIMITER = b'*'


class OrderManager:
    def __init__(self, host="127.0.0.1", port=10000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.order_id = 0
        self.lock = threading.Lock()

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        print(f"[OrderManager] Listening on {self.host}:{self.port}")

        while True:
            conn, addr = self.server_socket.accept()
            print(f"[OrderManager] Connected to Strategy at {addr}")
            threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()

    def handle_client(self, conn, addr):
        buffer = b""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data
                while MESSAGE_DELIMITER in buffer:
                    raw_msg, buffer = buffer.split(MESSAGE_DELIMITER, 1)
                    if not raw_msg:
                        continue
                    self.process_order(raw_msg.decode('utf-8'))
        except (ConnectionResetError, ConnectionAbortedError):
            print(f"[OrderManager] Client {addr} disconnected.")
        finally:
            conn.close()

    def process_order(self, msg):
        try:
            order = json.loads(msg)
            with self.lock:
                self.order_id += 1
                oid = self.order_id

            side = order.get("side")
            # Strategy sends 'size' key; older code expected 'qty'. Accept either.
            qty = order.get("qty") if order.get("qty") is not None else order.get("size")
            symbol = order.get("symbol")
            price = order.get("price")

            # defensive formatting
            side_str = side.upper() if side else "UNKNOWN"
            try:
                price_val = float(price) if price is not None else 0.0
            except Exception:
                price_val = 0.0

            print(f"Received Order {oid}: {side_str} {qty} {symbol} @ {price_val:.2f}")
        except Exception as e:
            print(f"[OrderManager] Failed to process order: {e}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="OrderManager TCP server")
    p.add_argument("--host", default="127.0.0.1", help="Host to bind")
    p.add_argument("--port", type=int, default=10000, help="Port to bind (default: 10000)")
    args = p.parse_args()

    om = OrderManager(host=args.host, port=args.port)
    om.start()

def main():
    import argparse
    p = argparse.ArgumentParser(description="OrderManager TCP server")
    p.add_argument("--host", default="127.0.0.1", help="Host to bind")
    p.add_argument("--port", type=int, default=10000, help="Port to bind (default: 10000)")
    args = p.parse_args()

    om = OrderManager(host=args.host, port=args.port)
    om.start()