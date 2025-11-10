"""
OrderBook:
- Connects to Gateway (TCP) to receive messages framed by MESSAGE_DELIMITER (b'*').
- Maintains latest prices for a list of symbols in multiprocessing.shared_memory as a NumPy structured array.
- Writes a small metadata file market_meta.json so other processes can attach to the same shared memory.
- Uses a file lock (market.lock) for atomic updates across independent processes.
- Reconnects to gateway if connection breaks.
"""

import socket
import time
import argparse
import json
import os
import sys
import fcntl
from multiprocessing import shared_memory
import numpy as np

HOST = "127.0.0.1"
GATEWAY_PORT = 9999
MESSAGE_DELIMITER = b'*'
META_FILE = "market_meta.json"
LOCK_FILE = "market.lock"
SHM_NAME_DEFAULT = "market_shm_v1"

def file_lock(fd):
    """Context manager for exclusive flock on a file descriptor."""
    class _LockCtx:
        def __init__(self, fd):
            self.fd = fd
        def __enter__(self):
            fcntl.flock(self.fd, fcntl.LOCK_EX)
            return self
        def __exit__(self, exc_type, exc, tb):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
    return _LockCtx(fd)

class OrderBook:
    def __init__(self, gateway_host, gateway_port, symbols, shm_name=SHM_NAME_DEFAULT, lock_path=LOCK_FILE):
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.symbols = [s.upper() for s in symbols]
        self.n = len(self.symbols)
        self.shm_name = shm_name
        self.lock_path = lock_path

        # dtype: fixed symbol bytes (S12) and float64 price
        self.dtype = np.dtype([('symbol', 'S12'), ('price', 'f8')])

        # create lock file if not exists
        open(self.lock_path, 'a').close()
        self.lock_fd = open(self.lock_path, 'r+')

        # create or attach shared memory
        self._create_or_attach_shm()

    def _create_or_attach_shm(self):
        recsize = self.dtype.itemsize
        total_bytes = recsize * self.n
        try:
            # try create new shared memory
            self.shm = shared_memory.SharedMemory(name=self.shm_name, create=True, size=total_bytes)
            print(f"[OrderBook] Created new shared memory '{self.shm_name}', size={total_bytes}")
            buf = np.ndarray((self.n,), dtype=self.dtype, buffer=self.shm.buf)
            # initialize
            for i, s in enumerate(self.symbols):
                buf['symbol'][i] = s.encode('utf-8')[:12]
                buf['price'][i] = 0.0
            # write metadata
            meta = {
                "shm_name": self.shm_name,
                "n": self.n,
                "dtype_descr": str(self.dtype.descr),
                "symbols": self.symbols
            }
            with open(META_FILE, 'w') as f:
                json.dump(meta, f)
            print(f"[OrderBook] Wrote meta file {META_FILE}")
        except FileExistsError:
            # already exists: attach
            self.shm = shared_memory.SharedMemory(name=self.shm_name, create=False)
            print(f"[OrderBook] Attached to existing shared memory '{self.shm_name}'")
            # TODO: could validate size vs expected

    def close(self):
        try:
            self.shm.close()
        except Exception:
            pass

    def _update_price(self, symbol: str, price: float):
        # atomic update with file lock
        with file_lock(self.lock_fd):
            arr = np.ndarray((self.n,), dtype=self.dtype, buffer=self.shm.buf)
            # find index
            sym_b = symbol.encode('utf-8')[:12]
            for i in range(self.n):
                if arr['symbol'][i].tobytes().rstrip(b'\x00') == sym_b:
                    arr['price'][i] = price
                    # debug print
                    # print(f"[OrderBook] Updated {symbol} -> {price}")
                    return True
            # if not found, ignore or add (we ignore)
            return False

    def run(self):
        # connect to gateway and process messages
        while True:
            try:
                print(f"[OrderBook] Connecting to Gateway {self.gateway_host}:{self.gateway_port} ...")
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.gateway_host, self.gateway_port))
                sock.settimeout(5.0)
                buffer = b''
                print("[OrderBook] Connected. Receiving ticks...")
                while True:
                    try:
                        data = sock.recv(4096)
                        if not data:
                            raise ConnectionError("socket closed by remote")
                        buffer += data
                        # parse by delimiter
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
                            # MESSAGE formats: SYMBOL,PRICE  or NEWS,SENTIMENT
                            parts = txt.split(',', 1)
                            if len(parts) != 2:
                                continue
                            key = parts[0].strip().upper()
                            val = parts[1].strip()
                            if key == "NEWS":
                                # OrderBook doesn't need to handle news
                                # but we could forward or store if desired
                                # print(f"[OrderBook] NEWS {val}")
                                continue
                            else:
                                try:
                                    price = float(val)
                                except ValueError:
                                    continue
                                updated = self._update_price(key, price)
                                if not updated:
                                    # unknown symbol: optionally log
                                    # print(f"[OrderBook] Unknown symbol {key}, ignoring")
                                    pass
                    except socket.timeout:
                        # continue to recv
                        continue
            except Exception as e:
                print("[OrderBook] Connection failure / exception:", e)
                try:
                    sock.close()
                except Exception:
                    pass
                print("[OrderBook] Reconnecting in 2s...")
                time.sleep(2.0)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--gateway-host", default=HOST)
    p.add_argument("--gateway-port", type=int, default=GATEWAY_PORT)
    p.add_argument("--symbols", default="AAPL,MSFT,GOOG,AMZN")
    p.add_argument("--shm-name", default=SHM_NAME_DEFAULT)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    ob = OrderBook(args.gateway_host, args.gateway_port, symbols, shm_name=args.shm_name)
    try:
        ob.run()
    except KeyboardInterrupt:
        print("\n[OrderBook] exiting (KeyboardInterrupt)")
    finally:
        ob.close()

def main():
    args = parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    ob = OrderBook(args.gateway_host, args.gateway_port, symbols, shm_name=args.shm_name)
    try:
        ob.run()
    except KeyboardInterrupt:
        print("\n[OrderBook] exiting (KeyboardInterrupt)")
    finally:
        ob.close()