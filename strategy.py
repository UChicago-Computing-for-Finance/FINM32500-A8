#!/usr/bin/env python3
"""
Strategy:
- Attaches to shared memory (market_meta.json) to read latest prices.
- Connects to Gateway as a TCP client to receive NEWS messages (sentiment).
- Maintains a rolling price buffer for a single symbol (or multiple, but we implement one by default).
- Computes simple moving averages (SMA short and long). Generates price signal:
    short_SMA > long_SMA -> BUY, else SELL (strict > or <).
- News signal: sentiment > bullish_threshold -> BUY; sentiment < bearish_threshold -> SELL.
- If both signals agree (both BUY or both SELL) and position differs, send an ORDER to OrderManager over TCP.
- Orders are serialized as JSON and delimited with MESSAGE_DELIMITER.
"""

import argparse
import json
import time
import socket
import threading
import collections
import os
import sys
from multiprocessing import shared_memory
import numpy as np
import fcntl

MESSAGE_DELIMITER = b'*'
META_FILE = "market_meta.json"
LOCK_FILE = "market.lock"

def file_lock(fd):
    class _LockCtx:
        def __init__(self, fd):
            self.fd = fd
        def __enter__(self):
            fcntl.flock(self.fd, fcntl.LOCK_SH)  # shared lock for reads
            return self
        def __exit__(self, exc_type, exc, tb):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
    return _LockCtx(fd)

class Strategy:
    def __init__(self,
                 symbol,
                 short_w=5,
                 long_w=20,
                 bullish_threshold=60,
                 bearish_threshold=40,
                 gateway_host='127.0.0.1',
                 gateway_port=9999,
                 order_manager_host='127.0.0.1',
                 order_manager_port=10000):
        self.symbol = symbol.upper()
        self.short_w = short_w
        self.long_w = long_w
        self.bullish_threshold = bullish_threshold
        self.bearish_threshold = bearish_threshold
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.order_manager_host = order_manager_host
        self.order_manager_port = order_manager_port

        # rolling buffer for prices (deque)
        self.buf = collections.deque(maxlen=self.long_w)
        self.position = None  # None, 'long', 'short'
        self.latest_sentiment = None
        self.stop = False

        # attach to shared memory using metadata
        if not os.path.exists(META_FILE):
            print(f"[Strategy] Meta file {META_FILE} not found. Make sure OrderBook created it.")
            sys.exit(1)
        with open(META_FILE, 'r') as f:
            meta = json.load(f)
        self.shm_name = meta['shm_name']
        self.symbols = meta['symbols']
        self.n = meta['n']
        # dtype consistent with orderbook
        self.dtype = np.dtype([('symbol', 'S12'), ('price', 'f8')])

        # attach
        self.shm = shared_memory.SharedMemory(name=self.shm_name, create=False)
        open(LOCK_FILE, 'a').close()
        self.lock_fd = open(LOCK_FILE, 'r+')

        # Launch thread to listen to Gateway (for NEWS)
        self.news_thread = threading.Thread(target=self._listen_gateway_news, daemon=True)
        self.news_thread.start()

    def close(self):
        self.stop = True
        try:
            self.shm.close()
        except Exception:
            pass

    def _get_latest_price(self):
        # read shared memory under shared lock
        with file_lock(self.lock_fd):
            arr = np.ndarray((self.n,), dtype=self.dtype, buffer=self.shm.buf)
            sym_b = self.symbol.encode('utf-8')[:12]
            for i in range(self.n):
                if arr['symbol'][i].tobytes().rstrip(b'\x00') == sym_b:
                    return float(arr['price'][i])
        return None

    def _listen_gateway_news(self):
        """Connect to Gateway and keep latest sentiment. We only care about NEWS messages."""
        while not self.stop:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.gateway_host, self.gateway_port))
                sock.settimeout(5.0)
                buffer = b''
                while not self.stop:
                    try:
                        data = sock.recv(4096)
                        if not data:
                            raise ConnectionError("gateway closed")
                        buffer += data
                        while True:
                            idx = buffer.find(MESSAGE_DELIMITER)
                            if idx == -1:
                                break
                            chunk = buffer[:idx]
                            buffer = buffer[idx+1:]
                            if not chunk:
                                continue
                            try:
                                txt = chunk.decode('utf-8')
                            except Exception:
                                continue
                            parts = txt.split(',', 1)
                            if len(parts) != 2:
                                continue
                            key = parts[0].strip().upper()
                            val = parts[1].strip()
                            if key == "NEWS":
                                try:
                                    sent = int(val)
                                    self.latest_sentiment = sent
                                    # debug
                                    # print(f"[Strategy] NEWS sentiment {sent}")
                                except ValueError:
                                    pass
                            # ignore price messages (we read prices from shared memory)
                    except socket.timeout:
                        continue
            except Exception as e:
                print("[Strategy] Gateway connection fail:", e)
                time.sleep(2.0)
                continue

    def _compute_price_signal(self):
        if len(self.buf) < self.long_w:
            return None  # insufficient history
        prices = list(self.buf)
        short_sma = sum(prices[-self.short_w:]) / self.short_w
        long_sma = sum(prices) / self.long_w
        if short_sma > long_sma:
            return "BUY"
        else:
            return "SELL"

    def _compute_news_signal(self):
        if self.latest_sentiment is None:
            return None
        if self.latest_sentiment > self.bullish_threshold:
            return "BUY"
        if self.latest_sentiment < self.bearish_threshold:
            return "SELL"
        return None

    def _send_order(self, side, price, size=100):
        order = {
            "type": "ORDER",
            "side": side,
            "symbol": self.symbol,
            "size": size,
            "price": price,
            "timestamp": time.time()
        }
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.order_manager_host, self.order_manager_port))
            payload = json.dumps(order).encode('utf-8') + MESSAGE_DELIMITER
            s.sendall(payload)
            # optional receive ack
            try:
                data = s.recv(4096)
                if data:
                    print("[Strategy] OrderManager replied:", data.decode('utf-8').strip('*'))
            except Exception:
                pass
            s.close()
            print(f"[Strategy] Sent order {side} {self.symbol} @ {price}")
        except Exception as e:
            print("[Strategy] Failed to send order:", e)

    def run(self, poll_interval=0.5):
        try:
            while True:
                price = self._get_latest_price()
                if price is not None:
                    self.buf.append(price)
                # compute signals only if enough history
                price_sig = self._compute_price_signal()
                news_sig = self._compute_news_signal()
                # debug
                # print(f"[Strategy] price_sig={price_sig}, news_sig={news_sig}, pos={self.position}")
                if price_sig and news_sig and price_sig == news_sig:
                    # both agree
                    if price_sig == "BUY" and self.position != "long":
                        # enter long
                        self._send_order("BUY", price)
                        self.position = "long"
                    elif price_sig == "SELL" and self.position != "short":
                        self._send_order("SELL", price)
                        self.position = "short"
                    else:
                        pass  # already in same position
                # else do nothing
                time.sleep(poll_interval)
        except KeyboardInterrupt:
            print("\n[Strategy] exiting (KeyboardInterrupt)")
        finally:
            self.close()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default=None, help="Symbol to trade (default: first symbol in market_meta.json)")
    p.add_argument("--short", type=int, default=5)
    p.add_argument("--long", type=int, default=20)
    p.add_argument("--bullish", type=int, default=60)
    p.add_argument("--bearish", type=int, default=40)
    p.add_argument("--gateway-host", default="127.0.0.1")
    p.add_argument("--gateway-port", type=int, default=9999)
    p.add_argument("--order-manager-host", default="127.0.0.1")
    p.add_argument("--order-manager-port", type=int, default=10000)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if not os.path.exists(META_FILE):
        print(f"[Strategy] {META_FILE} not found. Start OrderBook first.")
        sys.exit(1)
    with open(META_FILE, 'r') as f:
        meta = json.load(f)
    default_symbol = meta['symbols'][0]
    symbol = args.symbol.upper() if args.symbol else default_symbol
    strat = Strategy(symbol,
                     short_w=args.short,
                     long_w=args.long,
                     bullish_threshold=args.bullish,
                     bearish_threshold=args.bearish,
                     gateway_host=args.gateway_host,
                     gateway_port=args.gateway_port,
                     order_manager_host=args.order_manager_host,
                     order_manager_port=args.order_manager_port)
    strat.run()

def main():
    args = parse_args()
    if not os.path.exists(META_FILE):
        print(f"[Strategy] {META_FILE} not found. Start OrderBook first.")
        sys.exit(1)
    with open(META_FILE, 'r') as f:
        meta = json.load(f)
    default_symbol = meta['symbols'][0]
    symbol = args.symbol.upper() if args.symbol else default_symbol
    strat = Strategy(symbol,
                     short_w=args.short,
                     long_w=args.long,
                     bullish_threshold=args.bullish,
                     bearish_threshold=args.bearish,
                     gateway_host=args.gateway_host,
                     gateway_port=args.gateway_port,
                     order_manager_host=args.order_manager_host,
                     order_manager_port=args.order_manager_port)
    strat.run()