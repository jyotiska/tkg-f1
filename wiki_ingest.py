import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import time

from tqdm import tqdm
from bs4 import BeautifulSoup

class WikipediaIngester:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.base_url = "https://en.wikipedia.org/api/rest_v1"

    def get_page_content(self, title: str) -> Optional[Dict]:
        try:
            # Get page content
            content_url = f"{self.base_url}/page/html/{title}"
            response = requests.get(content_url)
            response.raise_for_status()

            # Get page info for revision ID
            info_url = f"https://en.wikipedia.org/w/api.php"
            info_params = {
                'action': 'query',
                'format': 'json',
                'titles': title,
                'prop': 'info|revisions',
                'rvprop': 'timestamp|ids'
            }

            info_response = requests.get(info_url, params=info_params)
            info_data = info_response.json()

            page_id = list(info_data['query']['pages'].keys())[0]
            page_info = info_data['query']['pages'][page_id]

            if 'revisions' not in page_info:
                print(f"No revisions found for {title}")
                return None

            revision = page_info['revisions'][0]

            return {
                'title': title,
                'page_id': page_id,
                'revision_id': revision['revid'],
                'timestamp': revision['timestamp'],
                'content': response.text,
                'extracted_at': datetime.now().isoformat()
            }

        except Exception as e:
            print(f"Error fetching {title}: {e}")
            return None

    def clean_html_content(self, html_content: str) -> str:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove unwanted elements
        for element in soup(['script', 'style', 'sup', 'table']):
            element.decompose()

        # Extract text from paragraphs
        paragraphs = soup.find_all('p')
        text_content = []

        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 50:  # Filter out very short paragraphs
                text_content.append(text)

        return '\n\n'.join(text_content)

    def save_page_data(self, page_data: Dict):
        jsonl_file = self.data_dir / "revisions.jsonl"
        page_data['clean_text'] = self.clean_html_content(page_data['content'])        
        del page_data['content']

        with open(jsonl_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(page_data) + '\n')

        text_file = self.data_dir / f"{page_data['title'].replace(' ', '_')}.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(page_data['clean_text'])

    def ingest_pages(self, page_titles: List[str]):
        print(f"Ingesting {len(page_titles)} Wikipedia pages...")

        for title in tqdm(page_titles, desc="Fetching pages"):
            page_data = self.get_page_content(title)
            if page_data:
                self.save_page_data(page_data)
                print(f"Saved: {title}")
            else:
                print(f"Failed: {title}")

            # Be nice to Wikipedia's servers
            time.sleep(1)

# F1 pages to scrape
F1_PAGES = [
    "Lewis Hamilton",
    "Max Verstappen", 
    "Sebastian Vettel",
    "Fernando Alonso",
    "Mercedes-AMG Petronas F1 Team",
    "Red Bull Racing",
    "Scuderia Ferrari",
    "2017 Formula One World Championship", 
    "2020 Formula One World Championship",
    "2021 Formula One World Championship",
    "Peter Bonnington",
    "Gianpiero Lambiase",
]

if __name__ == "__main__":
    ingester = WikipediaIngester()
    ingester.ingest_pages(F1_PAGES)
