from urllib.parse import urlparse, urldefrag
from bs4 import BeautifulSoup
import re

def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]
    with open("crawled_urls.txt", "a") as f:
        for link in valid_links:
            f.write(link + "\n")
    return valid_links

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = set()
    if resp.status != 200:
        print(f"Failed to fetch {url} with status code: {resp.status}")
        return list()

    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')

        for link in soup.find_all('a', href=True):
            href = link['href']
            # Resolve relative URLs
            abs_url = urldefrag(href)[0]
            links.add(abs_url)
    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        if not any(parsed.netloc.endswith(domain) for domain in ["ics.uci.edu", "cs.uci.edu",
                                                                 "informatics.uci.edu",
                                                                 "stat.uci.edu",
                                                                 "today.uci.edu"]):
            return False

        if any(keyword in url.lower() for keyword in [".pdf", "=", "?", "login"]):
            return False

        if re.search(r"/\d{4}-\d{2}-\d{2}", parsed.path) and any(kw in parsed.path.lower() for kw in ["events", "meeting", "calendar", "day"]):
            return False

        if re.search(r"/\d{4}-\d{2}", parsed.path) and any(kw in parsed.path.lower() for kw in ["events", "meeting", "calendar", "day"]):
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|jsp|bib|txt|rpm)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
