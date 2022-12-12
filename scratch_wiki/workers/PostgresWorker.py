import os
import threading
from queue import Empty

from sqlalchemy import create_engine
from sqlalchemy.sql import text


class PostgresMasterScheduler(threading.Thread):

    def __init__(self, input_queue, **kwargs) -> None:
        super(PostgresMasterScheduler, self).__init__(**kwargs)
        self._queue = input_queue
        self.start()

    def run(self) -> None:
        while True:
            try:
                value = self._queue.get(timeout=10)
            except Empty:
                print("Timeout reached in the postgres scheduler. Stopping...\n")
                break
            if value == "DONE":
                break
            symbol, price, extracted_time = value
            postgres_worker = PostgresWorker(symbol, price, extracted_time)
            postgres_worker.insert_into_db()
            print(f"{symbol, price, extracted_time} are stored in the postgres by {postgres_worker}")


class PostgresWorker:

    def __init__(self, symbol: str, price: float, extracted_time: int) -> None:
        self._symbol = symbol
        self._price = price
        self._extracted_time = extracted_time
        self._PG_USER = os.environ.get("PG_USER") or "postgres"
        self._PG_PASSWORD = os.environ.get("PG_PASSWORD") or ""
        self._PG_HOST = os.environ.get("PG_HOST") or "localhost"
        self._PG_DB = os.environ.get("PG_DB") or "concurrent_python"
        self._engine = create_engine(f"postgresql://{self._PG_USER}:{self._PG_PASSWORD}@{self._PG_HOST}/{self._PG_DB}")

    @staticmethod
    def _create_insert_query() -> str:
        sql = """INSERT INTO prices(symbol, price, insert_time) VALUES \
        (:symbol, :price, :insert_time)"""
        return sql

    @staticmethod
    def _create_select_query() -> str:
        sql = "SELECT * FROM PRICES WHERE symbol=:symbol"
        return sql

    @staticmethod
    def _create_update_query() -> str:
        sql = "UPDATE prices SET price = :price WHERE symbol = :symbol"
        return sql

    def insert_into_db(self):
        insert = self._create_insert_query()
        select = self._create_select_query()
        with self._engine.connect() as connection:

            select_result = connection.execute(text(select), {"symbol": self._symbol}).all()
            if select_result:
                update = self._create_update_query()
                connection.execute(text(update), {"symbol": self._symbol, "price": self._price})
            elif self._price is not None:
                connection.execute(text(insert), {"symbol": self._symbol, "price": self._price,
                                                  "insert_time": self._extracted_time})



