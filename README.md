# F1 Temporal Knowledge Graph 🏁

A Python project that builds a temporal knowledge graph from Formula 1 Wikipedia pages, extracting time-aware facts and storing them in Neo4j for querying.

## Features

- **Wikipedia Ingestion**: Fetches F1 pages and extracts text
- **Temporal Triple Extraction**: Uses OpenAI to extract time-aware facts
- **Neo4j Storage**: Stores relationships with temporal properties
- **Natural Language Queries**: Converts questions to Cypher queries
- **Web Interface**: Interactive query interface

## Prerequisites

- **Neo4j Desktop** - Download from [neo4j.com](https://neo4j.com)
- **OpenAI API Key** - Get from [platform.openai.com](https://platform.openai.com)
- **Python 3.8+**

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env file based on the .env.template file and add API key
OPENAI_API_KEY="your-api-key-here"

# Start Neo4j Desktop and create a new database
```

## Usage

```bash
# Check setup
python run.py check

# Run the complete pipeline
python run.py pipeline

# Start the web interface
python run.py web
```

Visit http://localhost:8000 to ask questions!

## Example Queries

- "Who was Lewis Hamilton's race engineer in 2017?"
- "What team did Max Verstappen drive for in 2020?"
- "Who won the championship in 2017?"
- "Which team dominated the 2020 season?"

## Project Structure

```
tkg-f1/
├── data/                   # Wikipedia content and extracted triples
├── templates/              # Web interface templates
├── logs/                   # Application logs
├── wiki_ingest.py         # Wikipedia scraping
├── triple_extraction.py   # OpenAI-based extraction
├── neo4j_loader.py        # Graph database loading
├── nl_to_cypher.py        # Query conversion
├── web_interface.py       # FastAPI web app
├── run.py                 # Main orchestration script
└── requirements.txt       # Python dependencies
```

## How It Works

1. **Wikipedia Ingestion** - Fetches F1 pages and extracts text
2. **Triple Extraction** - Uses OpenAI to extract temporal facts
3. **Neo4j Loading** - Stores relationships with time properties
4. **Query Interface** - Converts natural language to Cypher queries

## License

MIT License
