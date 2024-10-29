from threading import Thread
from urllib.parse import urlparse, urldefrag
import re

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

class Worker(Thread):
    def __init__(self, worker_id, config, frontier, shared_data=None):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.seen_hashes = set()
        self.shared_data = shared_data
        self.current_progress = 0
        self.MAX_URL_SIZE = 1024 * 1024  # Example threshold
        self.stop_words = {
            "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
            "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
            "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did",
            "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each",
            "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
            "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
            "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
            "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more",
            "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
            "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
            "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
            "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
            "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll",
            "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
            "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
            "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while",
            "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
            "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
        }
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

    def hash_content(self, content):
        hash_value = 0
        for char in content:
            hash_value = (hash_value * 31 + ord(char)) % (2 ** 32)  # Using a large prime base
        return hash_value

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

    def extract_words(self, content):
        text = re.sub(r'<[^>]+>', '', content)  # Remove HTML tags
        words = re.findall(r'\b\w+\b', text.lower())  # Extract words
        return [word for word in words if word not in self.stop_words]

    def process_page(self, url, content):
        parsed_url = urlparse(url)
        unique_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        self.shared_data['unique_urls'].add(unique_url)

        words = self.extract_words(content)
        word_count = len(words)

        if word_count > self.shared_data['longest_page']['word_count']:
            self.shared_data['longest_page']['url'] = unique_url
            self.shared_data['longest_page']['word_count'] = word_count

        if parsed_url.netloc.endswith("uci.edu"):
            subdomain = parsed_url.netloc.split('.')[0]
            self.shared_data['subdomain_counter'][subdomain] += 1

        self.shared_data['word_counter'].update(words)

    def run(self):
        while True:
            # add multiple threads to the frontier
            with self.frontier.lock:
                tbd_url = self.frontier.get_tbd_url()
                self.current_progress += 1
                if self.current_progress % 100 == 0:
                    print("------------------")
                    print("Progress: ", self.current_progress)
                    print("------------------")
                if not tbd_url:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                domain = urlparse(tbd_url).netloc
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
            time.sleep(self.config.time_delay)
