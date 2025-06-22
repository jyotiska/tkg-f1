import json
from pathlib import Path
from typing import List, Dict, Optional
from neo4j import GraphDatabase
from tqdm import tqdm

class Neo4jTemporalLoader:
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.data_dir = Path("data")

    def close(self):
        self.driver.close()

    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared")

    def create_indexes(self):
        with self.driver.session() as session:
            # Create index on entity names
            session.run("CREATE INDEX entity_name_index IF NOT EXISTS FOR (e:Entity) ON (e.name)")

            # Create index on years for temporal queries
            session.run("CREATE INDEX relationship_year_index IF NOT EXISTS FOR ()-[r:RELATION]-() ON (r.year)")

            print("Indexes created")

    def load_triple(self, triple: Dict) -> bool:
        try:
            with self.driver.session() as session:
                # Cypher query to create/merge entities and relationships
                query = """
                MERGE (subj:Entity {name: $subject})
                MERGE (obj:Entity {name: $object})
                MERGE (subj)-[r:RELATION {
                    type: $predicate,
                    year: $year,
                    source_page: $source_page,
                    extracted_at: $extracted_at
                }]->(obj)
                RETURN subj.name, r.type, obj.name
                """

                result = session.run(query, 
                    subject=triple['subject'],
                    object=triple['object'],
                    predicate=triple['predicate'],
                    year=triple.get('year'),
                    source_page=triple.get('source_page'),
                    extracted_at=triple.get('extracted_at')
                )

                return True

        except Exception as e:
            print(f"Error loading triple: {e}")
            print(f"Triple: {triple}")
            return False

    def load_triples_from_file(self, file_path: str = "data/triples.jsonl"):
        triples_file = Path(file_path)

        if not triples_file.exists():
            print(f"Triples file not found: {file_path}")
            return

        triples = []
        with open(triples_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    triple = json.loads(line.strip())
                    triples.append(triple)
                except json.JSONDecodeError:
                    continue

        print(f"Loading {len(triples)} triples into Neo4j...")

        successful = 0
        failed = 0

        for triple in tqdm(triples, desc="Loading triples"):
            if self.load_triple(triple):
                successful += 1
            else:
                failed += 1

        print(f"Successfully loaded: {successful}")
        print(f"Failed to load: {failed}")

        return successful, failed

    def get_database_stats(self) -> Dict:
        with self.driver.session() as session:
            # Count entities
            entity_count = session.run("MATCH (e:Entity) RETURN count(e) as count").single()['count']

            # Count relationships
            rel_count = session.run("MATCH ()-[r:RELATION]->() RETURN count(r) as count").single()['count']

            # Count temporal relationships (with years)
            temporal_count = session.run(
                "MATCH ()-[r:RELATION]->() WHERE r.year IS NOT NULL RETURN count(r) as count"
            ).single()['count']

            # Get year range
            year_stats = session.run(
                "MATCH ()-[r:RELATION]->() WHERE r.year IS NOT NULL RETURN min(r.year) as min_year, max(r.year) as max_year"
            ).single()

            return {
                'entities': entity_count,
                'relationships': rel_count,
                'temporal_relationships': temporal_count,
                'year_range': f"{year_stats['min_year']}-{year_stats['max_year']}" if year_stats['min_year'] else "N/A"
            }

    def run_sample_queries(self):
        with self.driver.session() as session:
            print("\nSample Queries:")

            # Query 1: Find Lewis Hamilton's relationships
            print("\n1. Lewis Hamilton's relationships:")
            result = session.run("""
                MATCH (lh:Entity {name: 'Lewis Hamilton'})-[r:RELATION]->(other)
                RETURN r.type, other.name, r.year
                ORDER BY r.year DESC
                LIMIT 5
            """)

            for record in result:
                year = record['r.year'] or 'Unknown'
                print(f"   {record['r.type']} → {record['other.name']} ({year})")

            # Query 2: Find relationships in specific year
            print("\n2. F1 relationships in 2017:")
            result = session.run("""
                MATCH (subj:Entity)-[r:RELATION]->(obj:Entity)
                WHERE r.year = 2017
                RETURN subj.name, r.type, obj.name
                LIMIT 5
            """)

            for record in result:
                print(f"   {record['subj.name']} {record['r.type']} {record['obj.name']}")

            # Query 3: Most connected entities
            print("\n3. Most connected entities:")
            result = session.run("""
                MATCH (e:Entity)-[r:RELATION]-()
                RETURN e.name, count(r) as connections
                ORDER BY connections DESC
                LIMIT 5
            """)

            for record in result:
                print(f"   {record['e.name']}: {record['connections']} connections")

def main():
    # Initialize loader
    loader = Neo4jTemporalLoader()

    try:
        print("Setting up Neo4j database...")

        # Optional: Clear existing data
        loader.clear_database()

        # Create indexes
        loader.create_indexes()

        # Load triples
        loader.load_triples_from_file()

        # Get stats
        stats = loader.get_database_stats()
        print("\nGraph Statistics:")
        print(f"   Entities: {stats['entities']}")
        print(f"   Relationships: {stats['relationships']}")
        print(f"   Temporal Relationships: {stats['temporal_relationships']}")
        print(f"   Year Range: {stats['year_range']}")

        # Run sample queries
        loader.run_sample_queries()

    finally:
        loader.close()

if __name__ == "__main__":
    # Make sure Neo4j Desktop is running locally
    # Default credentials: neo4j/password
    main()
