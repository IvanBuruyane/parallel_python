
import datetime
import threading
from queue import Empty
import requests
from bs4 import BeautifulSoup
from multiprocessing import Queue


class YahooFinanceScheduler(threading.Thread):

    def __init__(self, input_queue: Queue, output_queues: Queue, **kwargs) -> None:
        super(YahooFinanceScheduler, self).__init__(**kwargs)
        self._queue = input_queue
        temp_queue = output_queues
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queues = temp_queue
        self.start()

    def run(self) -> None:
        while True:
            try:
                value = self._queue.get(timeout=10)
            except Empty:
                print("Yahoo scheduler queue is empty. Stopping...")
                break
            if value == "DONE":
                break
            price_worker = YahooFinanceWorker(symbol=value)
            price = price_worker.get_price()
            output = (value, price, datetime.datetime.utcnow())
            for output_queue in self._output_queues:
                output_queue.put(output)
                print(f"{output} is put in the {output_queue} by {price_worker}")


class YahooFinanceWorker:

    def __init__(self, symbol: str, **kwargs) -> None:
        super(YahooFinanceWorker, self).__init__(**kwargs)
        self._symbol = self.__convert_symbol_to_yahoo_format(symbol)
        self._base_url = "https://finance.yahoo.com/quote/"
        self._url = f"{self._base_url}{self._symbol}"

    @staticmethod
    def __convert_symbol_to_yahoo_format(symbol: str) -> str:
        converted_symbol = symbol.replace(".", "-") if symbol.find(".") != -1 else symbol
        return converted_symbol

    def get_price(self) -> float:
        try:
            r = requests.get(self._url)
            assert r.status_code == 200
        except:
            print(f"GET {self._url} returned {r.status_code}, expected 200")
            return
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        try:
            price = soup.select_one(f"[data-field=regularMarketPrice][data-symbol={self._symbol}]").text
            return float(price)
        except:
            print(f"No element with tag [data-field=regularMarketPrice][data-symbol={self._symbol}]")

