import re
import os
from typing import Dict, List, Optional, Tuple
from neo4j import GraphDatabase
from openai import OpenAI
import json

class NLToCypherConverter:
    def __init__(self, neo4j_uri: str = "bolt://localhost:7687", 
                 neo4j_user: str = "neo4j", neo4j_password: str = "password",
                 openai_api_key: str = None):

        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.openai_client = OpenAI(api_key=openai_api_key) if openai_api_key else None

        # Rule-based patterns for common F1 queries
        self.query_patterns = [
            {
                'pattern': r"who was (.+?)'s? race engineer (?:when|in|for|during) (.+?)(?:\?|$)",
                'template': self._race_engineer_query
            },
            {
                'pattern': r"who was (.+?) race engineer for (before|after|during) (\d{4})(?:\?|$)",
                'template': self._race_engineer_reverse_relative_query
            },
            {
                'pattern': r"what team did (.+?) (?:drive for|race for) (?:in|during) (\d{4})(?:\?|$)",
                'template': self._team_query
            },
            {
                'pattern': r"who won (?:the )?championship (?:in|during) (\d{4})(?:\?|$)",
                'template': self._champion_query
            },
            {
                'pattern': r"which team dominated (?:the )?(\d{4}) season(?:\?|$)",
                'template': self._dominant_team_query
            },
            {
                'pattern': r"what team did (.+?) (?:drive for|race for) (?:before|after) (\d{4})(?:\?|$)",
                'template': self._team_relative_query
            },
            {
                'pattern': r"who won (?:the )?championship (?:before|after) (\d{4})(?:\?|$)",
                'template': self._champion_relative_query
            },
            {
                'pattern': r"which team dominated (?:the )?season (?:before|after) (\d{4})(?:\?|$)",
                'template': self._dominant_team_relative_query
            }
        ]

    def close(self):
        self.driver.close()

    def _parse_temporal_context(self, context: str) -> Tuple[Optional[int], Optional[str]]:
        # Extract year
        year_match = re.search(r'\d{4}', context)
        year = int(year_match.group()) if year_match else None

        return year

    def _build_temporal_filter(self, year: int, temporal_relation: str) -> str:
        if temporal_relation == 'before':
            return f"r.year < {year}"
        elif temporal_relation == 'after':
            return f"r.year > {year}"
        else:
            return f"r.year = {year}"

    def _race_engineer_query(self, driver_name: str, year_context: str) -> str:
        # Extract year from context
        year_match = re.search(r'\d{4}', year_context)
        year = int(year_match.group()) if year_match else None

        if year:
            return f"""
            MATCH (driver:Entity {{name: '{driver_name}'}})-[r:RELATION]->(engineer:Entity)
            WHERE r.type CONTAINS 'engineer' AND r.year = {year}
            RETURN engineer.name as answer, r.year as year
            """
        else:
            return f"""
            MATCH (driver:Entity {{name: '{driver_name}'}})-[r:RELATION]->(engineer:Entity)
            WHERE r.type CONTAINS 'engineer'
            RETURN engineer.name as answer, r.year as year
            ORDER BY r.year DESC
            LIMIT 1
            """

    def _race_engineer_reverse_relative_query(self, engineer_name: str, temporal_relation: str, year: str) -> str:
        print(engineer_name, temporal_relation, year)
        
        if temporal_relation in ['before', 'after']:
            temporal_filter = self._build_temporal_filter(int(year), temporal_relation)
            print(temporal_filter)
            order_clause = "ORDER BY r.year DESC" if temporal_relation == 'before' else "ORDER BY r.year ASC"
            
            return f"""
            MATCH (engineer:Entity {{name: '{engineer_name}'}})-[r:RELATION]->(driver:Entity)
            WHERE r.type CONTAINS 'engineer' AND {temporal_filter}
            RETURN driver.name as answer, r.year as year
            {order_clause}
            LIMIT 5
            """
        else:
            return f"""
            MATCH (engineer:Entity {{name: '{engineer_name}'}})-[r:RELATION]->(driver:Entity)
            WHERE r.type CONTAINS 'engineer' AND r.year = {year}
            RETURN driver.name as answer, r.year as year
            """

    def _team_query(self, driver_name: str, year: str) -> str:
        return f"""
        MATCH (driver:Entity {{name: '{driver_name}'}})-[r:RELATION]->(team:Entity)
        WHERE (r.type CONTAINS 'drove' OR r.type CONTAINS 'team' OR r.type CONTAINS 'race')
        AND r.year = {year}
        RETURN team.name as answer, r.year as year
        """

    def _team_relative_query(self, driver_name: str, year_context: str) -> str:
        year, temporal_relation = self._parse_temporal_context(year_context)

        if year and temporal_relation:
            temporal_filter = self._build_temporal_filter(year, temporal_relation)
            order_clause = "ORDER BY r.year DESC" if temporal_relation == 'before' else "ORDER BY r.year ASC"

            return f"""
            MATCH (driver:Entity {{name: '{driver_name}'}})-[r:RELATION]->(team:Entity)
            WHERE (r.type CONTAINS 'drove' OR r.type CONTAINS 'team' OR r.type CONTAINS 'race')
            AND {temporal_filter}
            RETURN team.name as answer, r.year as year
            {order_clause}
            LIMIT 5
            """
        else:
            return f"""
            MATCH (driver:Entity {{name: '{driver_name}'}})-[r:RELATION]->(team:Entity)
            WHERE (r.type CONTAINS 'drove' OR r.type CONTAINS 'team' OR r.type CONTAINS 'race')
            RETURN team.name as answer, r.year as year
            ORDER BY r.year DESC
            LIMIT 1
            """

    def _champion_query(self, year: str) -> str:
        return f"""
        MATCH (driver:Entity)-[r:RELATION]->(championship:Entity)
        WHERE (r.type CONTAINS 'champion' OR r.type CONTAINS 'won')
        AND r.year = {year}
        RETURN driver.name as answer, r.year as year
        """

    def _champion_relative_query(self, year_context: str) -> str:
        year, temporal_relation = self._parse_temporal_context(year_context)

        if year and temporal_relation:
            temporal_filter = self._build_temporal_filter(year, temporal_relation)
            order_clause = "ORDER BY r.year DESC" if temporal_relation == 'before' else "ORDER BY r.year ASC"

            return f"""
            MATCH (driver:Entity)-[r:RELATION]->(championship:Entity)
            WHERE (r.type CONTAINS 'champion' OR r.type CONTAINS 'won')
            AND {temporal_filter}
            RETURN driver.name as answer, r.year as year
            {order_clause}
            LIMIT 5
            """
        else:
            return f"""
            MATCH (driver:Entity)-[r:RELATION]->(championship:Entity)
            WHERE (r.type CONTAINS 'champion' OR r.type CONTAINS 'won')
            RETURN driver.name as answer, r.year as year
            ORDER BY r.year DESC
            LIMIT 1
            """

    def _dominant_team_query(self, year: str) -> str:
        return f"""
        MATCH (team:Entity)-[r:RELATION]->(achievement:Entity)
        WHERE r.year = {year}
        AND (r.type CONTAINS 'won' OR r.type CONTAINS 'champion' OR r.type CONTAINS 'dominated')
        RETURN team.name as answer, count(r) as dominance_score
        ORDER BY dominance_score DESC
        LIMIT 1
        """

    def _dominant_team_relative_query(self, year_context: str) -> str:
        year, temporal_relation = self._parse_temporal_context(year_context)

        if year and temporal_relation:
            temporal_filter = self._build_temporal_filter(year, temporal_relation)
            order_clause = "ORDER BY dominance_score DESC, r.year DESC" if temporal_relation == 'before' else "ORDER BY dominance_score DESC, r.year ASC"

            return f"""
            MATCH (team:Entity)-[r:RELATION]->(achievement:Entity)
            WHERE {temporal_filter}
            AND (r.type CONTAINS 'won' OR r.type CONTAINS 'champion' OR r.type CONTAINS 'dominated')
            RETURN team.name as answer, count(r) as dominance_score, r.year as year
            {order_clause}
            LIMIT 5
            """
        else:
            return f"""
            MATCH (team:Entity)-[r:RELATION]->(achievement:Entity)
            WHERE (r.type CONTAINS 'won' OR r.type CONTAINS 'champion' OR r.type CONTAINS 'dominated')
            RETURN team.name as answer, count(r) as dominance_score
            ORDER BY dominance_score DESC
            LIMIT 1
            """

    def rule_based_query(self, question: str) -> Optional[str]:
        print(f"Trying rule-based patterns for: '{question}'")
        for i, pattern_info in enumerate(self.query_patterns):
            match = re.search(pattern_info['pattern'], question.strip())
            # print(f"Pattern {i+1}: {pattern_info['pattern']}")
            # print(f"Match: {match}")
            if match:
                try:
                    print(f"Groups: {match.groups()}")
                    cypher = pattern_info['template'](*match.groups())
                    return cypher
                except Exception as e:
                    print(f"Error generating query: {e}")
                    continue

        return None

    def llm_based_query(self, question: str) -> Optional[str]:
        if not self.openai_client:
            return None

        # Get schema information
        schema_info = self._get_schema_info()

        system_prompt = """
        You are an expert at converting natural language questions about Formula 1 into Cypher queries for Neo4j.
        """

        prompt = f"""
Graph Schema:
- Nodes: Entity(name)
- Relationships: RELATION(type, year, source_page, extracted_at)

Common relationship types:
- "had_race_engineer", "is_race_engineer_for", "drove_for", "won_championship", "race_for_team"

Sample entities: "Lewis Hamilton", "Max Verstappen", "Peter Bonnington", "Mercedes-AMG Petronas F1 Team"

Convert this question to Cypher:
"{question}"

Rules:
1. Use exact entity names when possible
2. Filter by year when mentioned
3. For relative time queries:
   - "before 2020" → r.year < 2020
   - "after 2015" → r.year > 2015
   - Order by year DESC for "before" queries
   - Order by year ASC for "after" queries
4. Return meaningful aliases (answer, year)
5. Use CONTAINS in WHERE clause: WHERE r.type CONTAINS 'engineer'
6. Limit results to 5 for relative time queries
7. Always use proper Cypher syntax: MATCH (a:Entity)-[r:RELATION]->(b:Entity) WHERE conditions

Example queries:
- MATCH (driver:Entity {{name: 'Lewis Hamilton'}})-[r:RELATION]->(engineer:Entity) WHERE r.type CONTAINS 'engineer' AND r.year = 2017 RETURN engineer.name as answer, r.year as year
- MATCH (engineer:Entity {{name: 'Peter Bonnington'}})-[r:RELATION]->(driver:Entity) WHERE r.type CONTAINS 'engineer' AND r.year < 2017 RETURN driver.name as answer, r.year as year ORDER BY r.year DESC LIMIT 5

Return only the Cypher query, nothing else:
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=200
            )

            cypher = response.choices[0].message.content.strip()

            # Clean up the response
            if cypher.startswith('```'):
                cypher = cypher.split('\n')[1:-1]
                cypher = '\n'.join(cypher)

            return cypher

        except Exception as e:
            print(f"LLM query generation error: {e}")
            return None

    def _get_schema_info(self) -> Dict:
        with self.driver.session() as session:
            # Get sample relationship types
            result = session.run("""
            MATCH ()-[r:RELATION]->()
            RETURN DISTINCT r.type as rel_type
            LIMIT 10
            """)

            rel_types = [record['rel_type'] for record in result]

            # Get sample entity names
            result = session.run("""
            MATCH (e:Entity)
            RETURN e.name as name
            LIMIT 20
            """)

            entities = [record['name'] for record in result]

            return {
                'relationship_types': rel_types,
                'sample_entities': entities
            }

    def execute_query(self, cypher: str) -> List[Dict]:
        try:
            with self.driver.session() as session:
                result = session.run(cypher)
                return [dict(record) for record in result]
        except Exception as e:
            print(f"Query execution error: {e}")
            return []

    def answer_question(self, question: str, use_llm: bool = False) -> Dict:
        print(f"Question: {question}")

        # Try rule-based approach first
        cypher = self.rule_based_query(question)
        approach = "rule-based"
        
        # Fall back to LLM if rule-based fails
        if not cypher and use_llm:
            cypher = self.llm_based_query(question)
            approach = "llm-based"

        if not cypher:
            return {
                'question': question,
                'cypher': None,
                'results': [],
                'answer': "Sorry, I couldn't understand that question.",
                'approach': 'none'
            }

        print(f"Generated Cypher ({approach}):")
        print(f"   {cypher}")

        # Execute query
        results = self.execute_query(cypher)

        # Format answer
        answer = self._format_answer(results, question)

        return {
            'question': question,
            'cypher': cypher,
            'results': results,
            'answer': answer,
            'approach': approach
        }

    def _format_answer(self, results: List[Dict], question: str) -> str:
        if not results:
            return "I couldn't find an answer to that question in the knowledge graph."

        # Handle different types of results
        if len(results) == 1:
            result = results[0]
            if 'answer' in result:
                answer = result['answer']
                year = result.get('year', '')
                if year:
                    return f"{answer} (in {year})"
                else:
                    return str(answer)
            else:
                # Format first result
                return str(list(result.values())[0])

        else:
            # Multiple results - format as list
            answers = []
            for result in results[:5]:  # Limit to top 5
                if 'answer' in result:
                    answer = result['answer']
                    year = result.get('year', '')
                    if year:
                        answers.append(f"{answer} ({year})")
                    else:
                        answers.append(str(answer))
                else:
                    answers.append(str(list(result.values())[0]))

            return "; ".join(answers)

def demo_queries():
    # Sample questions to test
    test_questions = [
        "Who was Peter Bonnington race engineer for before 2024?"
        "Who was Lewis Hamilton's race engineer in 2017?",
        "What team did Max Verstappen drive for in 2020?",
        "Who won the championship in 2017?",
        "Which team dominated the 2020 season?",
        "Who was Lewis Hamilton's race engineer before 2017?",
        "What team did Max Verstappen drive for after 2018?",
        "Who won the championship before 2020?",
        "Which team dominated the season after 2015?"
    ]

    converter = NLToCypherConverter(
        openai_api_key=os.getenv('OPENAI_API_KEY')
    )

    try:
        print("F1 Temporal Knowledge Graph Demo\n")

        for question in test_questions:
            result = converter.answer_question(question, use_llm=True)
            print(f"Question: {question}")
            print(f"Answer: {result['answer']}")
            print(f"Approach: {result['approach']}")
            print("-" * 50)

    finally:
        converter.close()

if __name__ == "__main__":
    demo_queries()
    