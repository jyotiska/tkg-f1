import os
import sys
from pathlib import Path
from wiki_ingest import WikipediaIngester, F1_PAGES
from neo4j import GraphDatabase
from triple_extraction import TemporalTripleExtractor
from neo4j_loader import Neo4jTemporalLoader

import uvicorn
from web_interface import app

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def check_prerequisites():
    issues = []

    if not os.getenv('OPENAI_API_KEY'):
        issues.append("OPENAI_API_KEY not set")
    else:
        print("OpenAI API key found")

    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            session.run("RETURN 1")
        driver.close()
        print("Neo4j connection successful")
    except Exception as e:
        issues.append(f"Neo4j connection failed: {e}")

    return issues

def run_pipeline():
    print("F1 Temporal Knowledge Graph Pipeline")
    print("=" * 50)

    # Check prerequisites
    issues = check_prerequisites()
    if len(issues) > 0:
        print("Prerequisites not met:")
        for iss in issues:
            print(iss)
        print("\nPlease fix these issues before running the pipeline")
        return False

    # Step 1: Ingest Wikipedia data
    print("\nStep 1: Ingesting Wikipedia data...")
    try:
        ingester = WikipediaIngester()
        ingester.ingest_pages(F1_PAGES)
        print("Wikipedia data ingested")
    except Exception as e:
        print(f"Wikipedia ingestion failed: {e}")
        return False

    # Step 2: Extract triples
    print("\nStep 2: Extracting triples...")
    try:
        extractor = TemporalTripleExtractor(os.getenv('OPENAI_API_KEY'))
        total_triples = extractor.process_all_pages()
        print(f"Extracted {total_triples} triples")
    except Exception as e:
        print(f"Triple extraction failed: {e}")
        return False

    # Step 3: Load into Neo4j
    print("\nStep 3: Loading into Neo4j...")
    try:
        loader = Neo4jTemporalLoader()
        loader.create_indexes()
        successful, failed = loader.load_triples_from_file()

        print(f"Loaded {successful} triples")

        loader.close()
    except Exception as e:
        print(f"Neo4j loading failed: {e}")
        return False

    print("\nPipeline completed successfully!")
    return True

def start_web_interface():
    print("Starting web interface...")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        print(f"Failed to start web interface: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "pipeline":
            run_pipeline()
        elif command == "web":
            start_web_interface()
        elif command == "check":
            issues = check_prerequisites()
            if not issues:
                print("All prerequisites met!")
            else:
                for issue in issues:
                    print(issue)
        else:
            print("Usage: python run.py [pipeline|web|check]")
    else:
        print("F1 Temporal Knowledge Graph")
        print("Available commands:")
        print("  python run.py pipeline  - Run complete data pipeline")
        print("  python run.py web       - Start web interface")
        print("  python run.py check     - Check prerequisites")
