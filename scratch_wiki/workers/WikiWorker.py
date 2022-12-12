import threading
import requests
from bs4 import BeautifulSoup
from multiprocessing import Queue


class WikiMasterScheduler(threading.Thread):

    def __init__(self, output_queues: Queue, **kwargs) -> None:
        self._input_values = kwargs.pop("input_values")
        temp_queue = output_queues
        if type(temp_queue) != list:
            temp_queue = [temp_queue]
        self._output_queues = temp_queue
        super(WikiMasterScheduler, self).__init__(**kwargs)
        self.start()

    def run(self) -> None:
        for value in self._input_values:
            worker = WikiWorker(value)
            for symbol in worker.get_sp_500_companies():
                for output_queue in self._output_queues:
                    output_queue.put(symbol)
                    print(f"{symbol} is put in the {output_queue} by {worker}")


class WikiWorker:

    def __init__(self, url: str) -> None:
        self._url = url

    def get_sp_500_companies(self) -> list:
        r = requests.get(self._url)
        if r.status_code != 200:
            print(f"{r.status_code} {r.reason}")
            return []
        yield from self._extract_companies_symbols(r.text)

    @staticmethod
    def _extract_companies_symbols(html) -> list:
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find(id="constituents")
        table_rows = table.find_all("tr")
        for row in table_rows[1:]:
            symbol = row.find("td").text.strip("\n")
            yield symbol
