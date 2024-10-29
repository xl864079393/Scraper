from threading import Thread
import hashlib

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.seen_hashes = set()
        self.current_progress = 0
        self.MAX_URL_SIZE = 1024 * 1024  # Example threshold
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

    def hash_content(self, content):
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def Dead_Links(self, resp):
        if resp.status == 200:
            content_length = len(resp.raw_response.content)
            if content_length == 0 or content_length < 100:  # Example threshold
                return True
        return False

    def too_large(self, resp):
        if resp.status == 200:
            content_length = len(resp.raw_response.content)
            if content_length > self.MAX_URL_SIZE:
                return True
        return False
        
    def run(self):
        while True:
            # add multiple threads to the frontier
            with self.frontier.lock:
                tbd_url = self.frontier.get_tbd_url()
                if not tbd_url:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                domain = tbd_url.split("/")[2]
                if domain not in self.frontier.domain_last_time:
                    self.frontier.domain_last_time[domain] = time.time()
                elif time.time() - self.frontier.domain_last_time[domain] < self.config.time_delay:
                    time.sleep(self.config.time_delay - (time.time() - self.frontier.domain_last_time[domain]))
                    self.frontier.domain_last_time[domain] = time.time()

            print(f"Downloading {tbd_url}")
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")

            # Check if the URL is dead(Empty content or content length less than 100)
            if self.Dead_Links(resp):
                self.logger.warning(f"Dead URL detected: {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # Check if the URL is too large
            if self.too_large(resp):
                self.logger.warning(f"URL too large: {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            if resp.raw_response is None:
                self.logger.error(f"Failed to fetch {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            scraped_urls = scraper.scraper(tbd_url, resp)

            # Check if the URL is similar to a previously seen page
            content_hash = self.hash_content(resp.raw_response.content.decode('utf-8', 'ignore'))
            if content_hash not in self.seen_hashes:
                self.seen_hashes.add(content_hash)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            else:
                self.logger.info(f"Skipping similar page for URL: {tbd_url}")
            self.frontier.mark_url_complete(tbd_url)
            self.current_progress += 1
            if self.current_progress % 100 == 0:
                print("------------------")
                print("Progress: ", self.current_progress)
                print("------------------")
            time.sleep(self.config.time_delay)
