import importlib
import time

import yaml
import threading
from multiprocessing import Queue


class YamlPipelineReader(threading.Thread):

    def __init__(self, pipeline_location: str) -> None:
        super(YamlPipelineReader, self).__init__()
        self._pipeline_location = pipeline_location
        self._queues = {}
        self._workers = {}
        self._queue_consumers = {}
        self._downstream_queues = {}

    def _load_pipeline(self) -> None:
        with open(self._pipeline_location, "r") as file:
            self._yaml_data = yaml.safe_load(file)

    def _initialize_queues(self) -> None:
        for queue in self._yaml_data.get("queues"):
            queue_name = queue.get("name")
            self._queues[queue_name] = Queue()

    def _initialize_workers(self) -> None:
        for worker in self._yaml_data.get("workers"):
            WorkerClass = getattr(importlib.import_module(worker.get('location')), worker.get("class"))
            input_queue = worker.get("input_queue")
            output_queues = worker.get("output_queues")
            input_values = worker.get("input_values")
            num_instances = worker.get("instances", 1)
            init_params = {}
            if input_queue is not None:
                init_params["input_queue"] = self._queues.get(input_queue)
                self._queue_consumers[input_queue] = num_instances
            if output_queues is not None:
                init_params["output_queues"] = [self._queues.get(output_queue) for output_queue in output_queues]
            if input_values is not None:
                init_params["input_values"] = input_values
            worker_name = worker.get("name")
            self._downstream_queues[worker_name] = output_queues
            self._workers[worker_name] = []
            for i in range(num_instances):
                self._workers[worker_name].append(WorkerClass(**init_params))

    def _join_workers(self) -> None:
        for worker_name in self._workers:
            for worker_thread in self._workers.get(worker_name):
                worker_thread.join()

    def process_pipeline(self):
        self._load_pipeline()
        self._initialize_queues()
        self._initialize_workers()
        # self._join_workers()

    @property
    def queues(self):
        return self._queues

    def run(self) -> None:
        self.process_pipeline()
        # time.sleep(1)

        while True:
            total_workers_alive = 0
            worker_stats = []
            workers_to_delete = []
            for worker_name in self._workers:
                total_worker_threads_alive = 0
                for worker_thread in self._workers[worker_name]:
                    if worker_thread.is_alive():
                        total_worker_threads_alive += 1
                total_workers_alive += total_worker_threads_alive
                if not total_worker_threads_alive:
                    if self._downstream_queues[worker_name] is not None:
                        for output_queue in self._downstream_queues[worker_name]:
                            number_of_consumers = self._queue_consumers[output_queue]
                            for i in range(number_of_consumers):
                                self._queues[output_queue].put("DONE")

                    workers_to_delete.append(worker_name)

                worker_stats.append([worker_name, total_worker_threads_alive])
            if not total_workers_alive:
                break

            for worker_name in workers_to_delete:
                del self._workers[worker_name]

            time.sleep(0.5)



