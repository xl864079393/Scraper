from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from collections import Counter, defaultdict

shared_data = {
    'unique_urls': set(),
    'longest_page': {'url': None, 'word_count': 0},
    'word_counter': Counter(),
    'subdomain_counter': defaultdict(int),
}

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory


    def start_async(self):
        try:
            self.workers = [
                self.worker_factory(worker_id, self.config, self.frontier, shared_data)
                for worker_id in range(self.config.threads_count)]
            for worker in self.workers:
                worker.start()

            print(f"Unique pages found: {len(shared_data['unique_urls'])}")
            print(
                f"Longest page URL: {shared_data['longest_page']['url']} with {shared_data['longest_page']['word_count']} words.")
            print("50 most common words:", shared_data['word_counter'].most_common(50))

            print("Subdomains found in uci.edu:")
            for subdomain, count in sorted(shared_data['subdomain_counter'].items()):
                print(f"{subdomain}, {count}")

        except Exception as e:
            self.logger.error(f"Worker startup failed: {e}")

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
