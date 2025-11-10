"""
Gateway: TCP server that broadcasts price ticks and news sentiment.
Messages are framed with MESSAGE_DELIMITER (b'*').
Each message is a simple CSV-like string: "<SYMBOL>,<PRICE>*" or "NEWS,<SENTIMENT>*"
Example stream: b"AAPL,172.53*MSFT,325.20*NEWS,42*AAPL,172.61*..."
"""

import socket
import threading
import time
import random
import argparse
import csv
from typing import Dict, List

HOST = "0.0.0.0"
PORT = 9999
MESSAGE_DELIMITER = b'*'
BROADCAST_INTERVAL = 1.0  # seconds between ticks
NEWS_INTERVAL = 5.0       # seconds between news events

class GatewayServer:
    def __init__(self, host=HOST, port=PORT, symbols=None, initial_prices=None):
        self.host = host
        self.port = port
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow quick restart
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.client_sockets: List[socket.socket] = []
        self.clients_lock = threading.Lock()
        self.running = False

        self.symbols = symbols or ["AAPL", "MSFT", "GOOG", "AMZN"]
        # dict: symbol -> price (float)
        if initial_prices:
            self.prices: Dict[str, float] = {s: float(initial_prices.get(s, 100.0)) for s in self.symbols}
        else:
            # random initial prices
            self.prices = {s: round(random.uniform(50, 500), 2) for s in self.symbols}

    def start(self):
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen()
        print(f"[Gateway] Listening on {self.host}:{self.port}")
        self.running = True

        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()

        try:
            self._broadcast_loop()
        except KeyboardInterrupt:
            print("\n[Gateway] Shutting down (KeyboardInterrupt)...")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        with self.clients_lock:
            for c in self.client_sockets:
                try:
                    c.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    c.close()
                except Exception:
                    pass
            self.client_sockets.clear()
        try:
            self.server_sock.close()
        except Exception:
            pass
        print("[Gateway] Stopped.")

    def _accept_loop(self):
        while self.running:
            try:
                client_sock, addr = self.server_sock.accept()
                client_sock.setblocking(True)
                with self.clients_lock:
                    self.client_sockets.append(client_sock)
                print(f"[Gateway] Client connected: {addr}")
            except Exception as e:
                if self.running:
                    print(f"[Gateway] Accept error: {e}")
                break

    def _broadcast_loop(self):
        last_news_time = time.time()
        while self.running:
            t0 = time.time()

            # update prices with a simple random-walk model
            for sym in self.symbols:
                # relative change: small Gaussian
                change_pct = random.gauss(0, 0.0015)  # stdev ~0.15% per tick by default
                new_price = self.prices[sym] * (1 + change_pct)
                # keep to 2 decimals
                new_price = round(max(new_price, 0.01), 2)
                self.prices[sym] = new_price

            # send price messages (one message per symbol, delimited)
            with self.clients_lock:
                if self.client_sockets:
                    # create payload bytes for all symbols (each symbol ends with delimiter)
                    payload_parts = []
                    for sym, price in self.prices.items():
                        payload_parts.append(f"{sym},{price}".encode('utf-8') + MESSAGE_DELIMITER)
                    # broadcast each part
                    for part in payload_parts:
                        self._broadcast(part)

            # send news occasionally (NEWS,<sentiment>*). Use NEWS_INTERVAL.
            now = time.time()
            if now - last_news_time >= NEWS_INTERVAL:
                sentiment = random.randint(0, 100)
                news_msg = f"NEWS,{sentiment}".encode('utf-8') + MESSAGE_DELIMITER
                self._broadcast(news_msg)
                last_news_time = now
                # optionally jitter the next news interval
                # NEWS_INTERVAL could be randomized if desired

            # sleep to maintain broadcast interval, accounting for processing time
            elapsed = time.time() - t0
            to_sleep = max(0.0, BROADCAST_INTERVAL - elapsed)
            time.sleep(to_sleep)

    def _broadcast(self, msg: bytes):
        # attempts to send msg to all clients; remove clients that are closed/broken
        dead = []
        for c in list(self.client_sockets):
            try:
                c.sendall(msg)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
                # client disconnected
                dead.append(c)
            except Exception as e:
                # unexpected socket error: mark dead to cleanup
                print(f"[Gateway] Send error: {e}")
                dead.append(c)
        if dead:
            with self.clients_lock:
                for d in dead:
                    try:
                        peer = d.getpeername()
                    except Exception:
                        peer = "<unknown>"
                    print(f"[Gateway] Removing disconnected client {peer}")
                    try:
                        d.close()
                    except Exception:
                        pass
                    if d in self.client_sockets:
                        self.client_sockets.remove(d)

def load_prices_from_csv(csv_path: str) -> Dict[str, float]:
    prices = {}
    try:
        with open(csv_path, newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row: continue
                # expect: SYMBOL,PRICE in first two columns
                sym = row[0].strip()
                try:
                    price = float(row[1])
                except Exception:
                    continue
                prices[sym] = price
    except FileNotFoundError:
        raise
    return prices

def parse_args():
    p = argparse.ArgumentParser(description="Gateway: price + news TCP broadcaster")
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", default=PORT, type=int)
    p.add_argument("--csv", default=None, help="Optional CSV file with SYMBOL,PRICE per row")
    p.add_argument("--symbols", default=None, help="Comma-separated symbol list (overrides CSV)")
    p.add_argument("--interval", default=None, type=float, help="Broadcast interval in seconds")
    p.add_argument("--news-interval", default=None, type=float, help="News interval in seconds")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.interval:
        BROADCAST_INTERVAL = args.interval
    if args.news_interval:
        NEWS_INTERVAL = args.news_interval

    initial_prices = None
    symbols = None
    if args.csv:
        try:
            initial_prices = load_prices_from_csv(args.csv)
            symbols = list(initial_prices.keys())
            print(f"[Gateway] Loaded {len(symbols)} symbols from CSV.")
        except Exception as e:
            print(f"[Gateway] Failed to load CSV {args.csv}: {e}")
            raise

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]

    gw = GatewayServer(host=args.host, port=args.port, symbols=symbols, initial_prices=initial_prices)
    gw.start()

def main():
    args = parse_args()
    if args.interval:
        BROADCAST_INTERVAL = args.interval
    if args.news_interval:
        NEWS_INTERVAL = args.news_interval

    initial_prices = None
    symbols = None
    if args.csv:
        try:
            initial_prices = load_prices_from_csv(args.csv)
            symbols = list(initial_prices.keys())
            print(f"[Gateway] Loaded {len(symbols)} symbols from CSV.")
        except Exception as e:
            print(f"[Gateway] Failed to load CSV {args.csv}: {e}")
            raise

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]

    gw = GatewayServer(host=args.host, port=args.port, symbols=symbols, initial_prices=initial_prices)
    gw.start()