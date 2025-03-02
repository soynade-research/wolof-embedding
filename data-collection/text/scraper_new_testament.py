import requests
from bs4 import BeautifulSoup
from datasets import Dataset
from tqdm import tqdm
import time
import logging
from typing import List, Dict, Optional
from utils import get_html, create_and_push_dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NewTestamentScraper:
    def __init__(self, urls: List[str], max_retries: int = 3, delay: int = 0, timeout:int = 15):
        self.urls = urls
        self.source = 'New Testament'
        self.max_retries = max_retries
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Your user agent", #Some websites block requests without a valid User-Agent or from unknown clients.
        })


    def scrape_article(self, url: str) -> Optional[Dict]:
        """Extract and join all paragraph content from a single page"""
        soup = get_html(url, self.session, self.delay, self.max_retries, self.timeout)
        if not soup:
            return 
            
        try:
            paragraphs = soup.find_all('p')
            content = '\n\n'.join([
                p.get_text(strip=True) 
                for p in paragraphs 
                if p.get_text(strip=True)
            ])
            
            return {
                'url': url,
                'content': content,
                'source': self.source
            }
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return 

    def process_articles(self) -> List[Dict]:
        """Process all URLs with progress tracking"""
        articles = []
        for url in tqdm(self.urls, desc="Scraping New Testament books"):
            if article := self.scrape_article(url):
                articles.append(article)
        return articles

    def create_dataset(self, dataset_name: str):
        """Create and push Hugging Face dataset"""
        articles = self.process_articles()

        create_and_push_dataset(articles, dataset_name)



if __name__ == "__main__":
    testament_urls = [
        'https://sacred-texts.com/bib/wb/wlf/mat.htm',
        'https://sacred-texts.com/bib/wb/wlf/mar.htm',
        'https://sacred-texts.com/bib/wb/wlf/luk.htm',
        'https://sacred-texts.com/bib/wb/wlf/joh.htm',
        'https://sacred-texts.com/bib/wb/wlf/act.htm',
        'https://sacred-texts.com/bib/wb/wlf/rom.htm',
        'https://sacred-texts.com/bib/wb/wlf/co1.htm',
        'https://sacred-texts.com/bib/wb/wlf/co2.htm',
        'https://sacred-texts.com/bib/wb/wlf/gal.htm',
        'https://sacred-texts.com/bib/wb/wlf/eph.htm',
        'https://sacred-texts.com/bib/wb/wlf/phi.htm',
        'https://sacred-texts.com/bib/wb/wlf/col.htm',
        'https://sacred-texts.com/bib/wb/wlf/th1.htm',
        'https://sacred-texts.com/bib/wb/wlf/th2.htm',
        'https://sacred-texts.com/bib/wb/wlf/ti1.htm',
        'https://sacred-texts.com/bib/wb/wlf/ti2.htm',
        'https://sacred-texts.com/bib/wb/wlf/tit.htm',
        'https://sacred-texts.com/bib/wb/wlf/plm.htm',
        'https://sacred-texts.com/bib/wb/wlf/heb.htm',
        'https://sacred-texts.com/bib/wb/wlf/jam.htm',
        'https://sacred-texts.com/bib/wb/wlf/pe1.htm',
        'https://sacred-texts.com/bib/wb/wlf/pe2.htm',
        'https://sacred-texts.com/bib/wb/wlf/jo1.htm',
        'https://sacred-texts.com/bib/wb/wlf/jo2.htm',
        'https://sacred-texts.com/bib/wb/wlf/jo3.htm',
        'https://sacred-texts.com/bib/wb/wlf/jde.htm',
        'https://sacred-texts.com/bib/wb/wlf/rev.htm'
    ]

    scraper = NewTestamentScraper(
        urls=testament_urls
    )
    
    dataset = scraper.create_dataset(dataset_name="soynade-research/WolofEmb-NewTestament")