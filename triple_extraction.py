import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from openai import OpenAI
from tqdm import tqdm
import time

class TemporalTripleExtractor:
    def __init__(self, api_key: str, data_dir: str = "data"):
        self.client = OpenAI(api_key=api_key)
        self.data_dir = Path(data_dir)
        self.triples_file = self.data_dir / "triples.jsonl"
        
    def extract_year_from_sentence(self, sentence: str) -> Optional[int]:
        # Look for 4-digit years (1950-2030)
        year_patterns = [
            r'\b(19[5-9]\d|20[0-3]\d)\b',
            r'in\s+(19[5-9]\d|20[0-3]\d)',
            r'during\s+(19[5-9]\d|20[0-3]\d)',
            r'season\s+(19[5-9]\d|20[0-3]\d)'
        ]

        for pattern in year_patterns:
            match = re.search(pattern, sentence)
            if match:
                year = int(match.group(1) if match.groups() else match.group(0))
                if 1950 <= year <= 2030:
                    return year
        return None

    def extract_triples_from_text(self, text: str, page_title: str) -> List[Dict]:
        # Split text into sentences (basic approach)
        sentences = [s.strip() for s in text.split('.') if len(s.strip()) > 20]

        all_triples = []

        # Process in batches to manage API costs
        batch_size = 5
        for i in tqdm(range(0, len(sentences), batch_size), desc=f"Processing {page_title}"):
            batch = sentences[i:i+batch_size]
            batch_text = '. '.join(batch)

            try:
                triples = self._extract_batch_triples(batch_text, page_title)
                all_triples.extend(triples)

                # Rate limiting
                time.sleep(0.5)
            except Exception as e:
                print(f"Error processing batch: {e}")
                continue

        return all_triples

    def _extract_batch_triples(self, text: str, page_title: str) -> List[Dict]:
        system_prompt = """
        You are an expert at extracting structured knowledge from Formula 1 text. Return only valid JSON.
        """

        prompt = f"""
Extract factual relationships from this Formula 1 text as subject-predicate-object triples.

Rules:
1. Focus on factual relationships (not opinions)
2. Use canonical names (e.g., "Lewis Hamilton" not "Hamilton")
3. Make predicates descriptive (e.g., "drove_for", "won_championship_in", "had_race_engineer")
4. Extract only clear, unambiguous facts
5. Return as JSON list: [{{"subject": "X", "predicate": "Y", "object": "Z"}}]

Text: {text}

Context: This is from the Wikipedia page about "{page_title}".

JSON Response:
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )

            content = response.choices[0].message.content.strip()

            # Parse JSON response
            if content.startswith('```json'):
                content = content[7:-3]
            elif content.startswith('```'):
                content = content[3:-3]

            triples_data = json.loads(content)

            # Add temporal and source information
            enriched_triples = []
            for triple in triples_data:
                if all(key in triple for key in ['subject', 'predicate', 'object']):
                    # Try to extract year from the original text
                    year = self.extract_year_from_sentence(text)

                    enriched_triple = {
                        'subject': triple['subject'],
                        'predicate': triple['predicate'], 
                        'object': triple['object'],
                        'year': year,
                        'source_page': page_title,
                        'extracted_at': datetime.now().isoformat()
                    }
                    enriched_triples.append(enriched_triple)

            return enriched_triples

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            print(f"Raw response: {content}")
            return []
        except Exception as e:
            print(f"API error: {e}")
            return []

    def save_triples(self, triples: List[Dict]):
        with open(self.triples_file, 'a', encoding='utf-8') as f:
            for triple in triples:
                f.write(json.dumps(triple) + '\n')

    def process_all_pages(self):
        text_files = list(self.data_dir.glob("*.txt"))
        print(f"Processing {len(text_files)} text files...")
        total_triples = 0

        for text_file in text_files:
            page_title = text_file.stem.replace('_', ' ')
            print(f"\nProcessing: {page_title}")

            with open(text_file, 'r', encoding='utf-8') as f:
                text = f.read()

            triples = self.extract_triples_from_text(text, page_title)

            if triples:
                self.save_triples(triples)
                print(f"Extracted {len(triples)} triples from {page_title}")
                total_triples += len(triples)
            else:
                print(f"No triples extracted from {page_title}")

        print(f"\nTotal triples extracted: {total_triples}")
        return total_triples

# Example usage and cost estimation
def estimate_cost(num_sentences: int) -> float:
    # Assuming ~100 tokens per sentence for input + output
    tokens_per_sentence = 200
    total_tokens = num_sentences * tokens_per_sentence

    # GPT-4o-mini pricing (as of 2024)
    cost_per_1k_tokens = 0.00015  # Input tokens
    cost_per_1k_tokens_output = 0.0006  # Output tokens

    estimated_cost = (total_tokens / 1000) * (cost_per_1k_tokens + cost_per_1k_tokens_output)
    return estimated_cost

if __name__ == "__main__":
    # Usage example:
    # export OPENAI_API_KEY=your_key_here
    # python triple_extraction.py

    import os
    api_key = os.getenv('OPENAI_API_KEY')

    if not api_key:
        print("Please set OPENAI_API_KEY environment variable")
        exit(1)

    extractor = TemporalTripleExtractor(api_key)
    extractor.process_all_pages()
    