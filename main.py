# To run this system, you generally launch each component (gateway, orderbook, strategy, ordermanager)
# in separate terminals or using process managers (like tmux, supervisord, or scripts).
#
# Example CLI startup in four terminals:
#   python gateway.py
#   python orderbook.py
#   python strategy.py
#   python ordermanager.py
#
# For test scripts and automation, you could use multiprocessing (as below), but production
# usage is usually one process per terminal.
#
# Example (not recommended for debugging):
#
from multiprocessing import Process
import time

def run_gateway():
    import gateway
    gateway.main()

def run_orderbook():
    import orderbook
    orderbook.main()

def run_strategy():
    import strategy
    strategy.main()

def run_ordermanager():
    import order_manager
    order_manager.main()

if __name__ == "__main__":

    processes = [
        Process(target=run_gateway),

        Process(target=run_orderbook),

        Process(target=run_strategy),

        Process(target=run_ordermanager)]

    for p in processes: p.start()

    for p in processes: p.join()
