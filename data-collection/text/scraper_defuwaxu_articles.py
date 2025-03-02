import requests
from urllib.parse import urlparse, urlunparse, urljoin
import re
from typing import List, Set, Dict, Optional
from tqdm import tqdm
import time
import logging
from utils import get_html, create_and_push_dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DefuwaxuArticleScraper:
    def __init__(
        self,
        categories: List[str],
        max_retries: int = 3,
        delay: int = 0,
        request_timeout: int = 10,
    ):
        self.categories = categories
        self.max_retries = max_retries
        self.delay = delay
        self.timeout = request_timeout
        self.all_article_urls: Set[str] = set()
        self.articles_data: List[Dict] = []

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Your user agent", #Some websites block requests without a valid User-Agent or from unknown clients.
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

    def _handle_pagination(self, url: str, page: int) -> str:
        parsed = list(urlparse(url))
        path = parsed[2]

        if re.search(r"/page/\d+/", path):
            new_path = re.sub(r"/page/\d+/", f"/page/{page}/", path)
        else:
            new_path = f"{path.rstrip('/')}/page/{page}/" if page > 1 else path
        parsed[2] = new_path
        return urlunparse(parsed)

    def extract_urls_from_page(self, url: str) -> Set[str]:
        soup = get_html(url, self.session, self.delay, self.max_retries, self.timeout)
        if not soup:
            return set()
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        urls = set()

        for link in soup.select("div.tdb_module_loop a[href]"):
            try:
                absolute_url = urljoin(base_url, link["href"])
                if re.match(r"^https?://", absolute_url):
                    urls.add(absolute_url)
            except ValueError:
                logger.warning(f"Invalid URL found: {link['href']}")
        return urls

    def get_article_content(self, url: str) -> Dict:
        """Extract content from an article URL."""
        try:
            soup = get_html(url, self.session, self.delay, self.max_retries, self.timeout)
            article = soup.find("article")

            if not article:
                return 
            text_elements = article.find_all(["h1", "h2", "p"])
            content = []

            for element in text_elements:
                if not element.text.strip():
                    continue
                if any(
                    cls in element.get("class", [])
                    for cls in [
                        "nav",
                        "menu",
                        "sidebar",
                        "footer",
                        "header",
                        "metadata",
                    ]
                ):
                    continue
                text = element.text.strip()
                content.append(text)
            seen = set()
            unique_content = [x for x in content if not (x in seen or seen.add(x))]

            return {
                "url": url,
                "content": "\n\n".join(unique_content),
                "Source": "Defuwaxu",
            }
        except Exception as e:
            logger.error(f"Error processing article {url}: {str(e)}")
            return

    def scrape_category(self, category_url: str) -> None:
        page = 1
        max_consecutive_empty = 2
        consecutive_empty = 0

        while True:
            page_url = self._handle_pagination(category_url, page)
            logger.info(f"Scraping category page {page}: {page_url}")

            urls = self.extract_urls_from_page(page_url)

            if not urls:
                consecutive_empty += 1
                logger.info(f"Empty page {page} (consecutive: {consecutive_empty})")
                if consecutive_empty >= max_consecutive_empty:
                    break
                page += 1
                continue
            new_urls = urls - self.all_article_urls
            if not new_urls:
                logger.info("No new URLs found, ending pagination")
                break
            self.all_article_urls.update(new_urls)
            logger.info(f"Added {len(new_urls)} new articles from page {page}")
            consecutive_empty = 0
            page += 1

            if page > 20:
                logger.warning("Reached safety page limit (20)")
                break

    def scrape_all_categories(self) -> None:
        logger.info("Starting category scraping...")
        for category_url in tqdm(self.categories, desc="Scraping categories"):
            self.scrape_category(category_url)
        logger.info(f"Total unique article URLs found: {len(self.all_article_urls)}")

    def scrape_all_articles(self) -> None:
        logger.info("Starting article content extraction...")
        for url in tqdm(self.all_article_urls, desc="Scraping articles"):
            if article_data := self.get_article_content(url):
                self.articles_data.append(article_data)
        logger.info(f"Successfully scraped {len(self.articles_data)} articles")

    def create_dataset(
        self, dataset_name: str
    ) -> None:

        create_and_push_dataset(self.articles_data, dataset_name)


    def run(self, dataset_name: str) -> None:
        self.scrape_all_categories()
        self.scrape_all_articles()
        self.create_dataset(dataset_name)


if __name__ == "__main__":
    categories = [
        "https://www.defuwaxu.com/category/magu-waxoon-naa-ko/page/1/",  # 3 pages
        "https://www.defuwaxu.com/category/fattaliku-demb/page/1",  # 1 page
        "https://www.defuwaxu.com/category/waxleen-seen-xalaat/page/1/",  # 2 pages
        "https://www.defuwaxu.com/category/xibaar/bayyi-ci-xel/page/1",  # 1 page
        "https://www.defuwaxu.com/category/xibaar/bindkatu-ayubes-bi/page/1",  # 1 page
        "https://www.defuwaxu.com/category/xibaar/diine/page/1",  # 1 page
        "https://www.defuwaxu.com/category/xibaar/gis-gis/page/1/",  # 6 pages
        "https://www.defuwaxu.com/category/xibaar/kii-kumu/page/1",  # 1 page
        "https://www.defuwaxu.com/category/xibaar/koom-koom/page/1",  # 1 page
        "https://www.defuwaxu.com/category/xibaar/politig/page/1",  # 1 Page
        "https://www.defuwaxu.com/category/xibaar/taggat-yaram/page/1/",  # 8 page
    ]

    scraper = DefuwaxuArticleScraper(
        categories, max_retries=3, delay=0, request_timeout=15
    )
    scraper.run(dataset_name="soynade-research/WolofEmb-Defuwaxu")
