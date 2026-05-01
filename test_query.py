import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def run_test_query():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    # "Find me a director in Hyderabad who works in mass entertainers and has worked with Mythri"
    # We map this to:
    # 1. Craft contains "director"
    # 2. Location contains "hyderabad"
    # 3. Banner contains "mythri" (using AFFILIATED_WITH which covers direct and project-based affiliations)
    # 4. Tags or Project types contain "mass"
    
    query = """
    MATCH (u:User)-[:HAS_PRIMARY_CRAFT]->(c:Craft)
    WHERE toLower(c.name) CONTAINS 'director'
    
    MATCH (u)-[:LIVES_IN]->(l:Location)
    WHERE toLower(l.name) CONTAINS 'hyderabad'
    
    MATCH (u)-[:CREDITED_ON]->(p:Project)<-[:PRODUCED]-(b:Banner)
    WHERE toLower(b.name) CONTAINS 'mythri'
    
    // Check if "mass" is in their tags or project types
    WITH u, c, l, b, collect(p.type) as project_types
    WHERE any(tag IN u.tags_self WHERE toLower(tag) CONTAINS 'mass') 
       OR any(ptype IN project_types WHERE toLower(ptype) CONTAINS 'mass')
       OR toLower(u.bio) CONTAINS 'mass'
       
    RETURN u.name AS Name, c.name AS Craft, l.name AS Location, b.name AS Banner
    """
    
    try:
        with driver.session() as session:
            result = session.run(query)
            records = list(result)
            
            print(f"Found {len(records)} matches:")
            for record in records:
                print(f"- {record['Name']} ({record['Craft']}, {record['Location']}) - Worked with {record['Banner']}")
                
    finally:
        driver.close()

if __name__ == "__main__":
    run_test_query()
