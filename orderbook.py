'''
3. OrderBook (Shared Market State)
Responsibilities:

Connects to the Gateway to receive price data.

Maintains the latest prices for all symbols in shared memory.

Provides shared memory access to the Strategy process.

Expectations:

Use multiprocessing.shared_memory to store and update a NumPy structured array, or a serialized dictionary of {symbol: price}.

Ensure updates are atomic and synchronized using multiprocessing.Lock.

Handle reconnection logic gracefully if the Gateway restarts.
'''

import time

print("OrderBook")
for i in range(10):
    print(str(i) + " OrderBook")
    time.sleep(1)