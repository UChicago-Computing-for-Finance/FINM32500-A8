from multiprocessing import Process
from shared_memory_utils import SharedPriceBook

if __name__ == "__main__":

    processes = [

        Process(target=run_gateway),

        Process(target=run_orderbook),

        Process(target=run_strategy),

        Process(target=run_ordermanager)

    ]

    for p in processes: p.start()

    for p in processes: p.join()