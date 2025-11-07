'''
Responsibilities:

Acts as a server that broadcasts two streams:

Price stream: A random-walk price feed for multiple symbols. You can build this yourself, or you can use the csv file from the previous homework as the price data you will stream.

News sentiment: Integers between 0–100, representing market sentiment. Low values represent bad news while high values represent good news. A sentiment of 50 is neutral. You will be the news server yourself that will randomly choose a news sentiment value to stream.

Uses TCP sockets to send data to connected clients (OrderBook and Strategy).
'''

import time
print("Gateway")
for i in range(10):
    print(str(i) + " Gateway")
    time.sleep(1)