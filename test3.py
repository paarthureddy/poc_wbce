import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
USER = os.getenv('NEO4J_USER', 'neo4j')
PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')

d = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
with d.session() as s:
    res = s.run('MATCH ()-[r:AFFILIATED_WITH]->() RETURN count(r) as c').single()
    print('Total AFFILIATED_WITH edges:', res['c'])
    if res['c'] > 0:
        sample = s.run('MATCH (u:User)-[r:AFFILIATED_WITH]->(b:Banner) RETURN u.name as User, b.name as Banner LIMIT 5').data()
        for x in sample:
            print(f"- {x['User']} affiliated with {x['Banner']}")
d.close()
