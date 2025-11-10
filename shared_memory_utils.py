from multiprocessing import shared_memory
import numpy as np
import json
import os
import fcntl

SHM_NAME_DEFAULT = "market_shm_v1"
META_FILE = "market_meta.json"
LOCK_FILE = "market.lock"

class SharedPriceBook:

    def __init__(self, symbols, name=None):
        self.name = name or SHM_NAME_DEFAULT
        
        # Load metadata from market_meta.json
        if not os.path.exists(META_FILE):
            raise FileNotFoundError(f"Meta file {META_FILE} not found. Make sure OrderBook created it.")
        
        with open(META_FILE, 'r') as f:
            meta = json.load(f)
        
        self.shm_name = meta['shm_name']
        self.symbols = meta['symbols']
        self.n = meta['n']
        # dtype consistent with orderbook
        self.dtype = np.dtype([('symbol', 'S12'), ('price', 'f8')])
        
        # Attach to shared memory
        self.shm = shared_memory.SharedMemory(name=self.shm_name, create=False)
        
        # Set up file lock for safe reads
        open(LOCK_FILE, 'a').close()
        self.lock_fd = open(LOCK_FILE, 'r+')

    def update(self, symbol, price):
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)
        try:
            arr = np.ndarray((self.n,), dtype=self.dtype, buffer=self.shm.buf)
            sym_b = symbol.upper().encode('utf-8')[:12]
            for i in range(self.n):
                if arr['symbol'][i].tobytes().rstrip(b'\x00') == sym_b:
                    arr['price'][i] = price
                    fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                    return True
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
        except Exception:
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            raise



    def read(self, symbol):
        # Read shared memory under shared lock
        fcntl.flock(self.lock_fd, fcntl.LOCK_SH)  # shared lock for reads
        try:
            arr = np.ndarray((self.n,), dtype=self.dtype, buffer=self.shm.buf)
            sym_b = symbol.upper().encode('utf-8')[:12]
            for i in range(self.n):
                if arr['symbol'][i].tobytes().rstrip(b'\x00') == sym_b:
                    return float(arr['price'][i])
            return None  # symbol not found
        finally:
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)