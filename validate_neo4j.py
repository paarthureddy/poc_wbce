import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def run_checks():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    success = True
    
    try:
        with driver.session() as session:
            # Check 1: Total User node count = 200
            res1 = session.run("MATCH (u:User) RETURN count(u) as count").single()
            count1 = res1["count"]
            if count1 == 200:
                print(f"Check 1 PASS: Found exactly 200 users.")
            else:
                print(f"Check 1 FAIL: Expected 200 users, found {count1}.")
                success = False

            # Check 2: Every User has exactly one HAS_PRIMARY_CRAFT edge
            res2 = session.run("""
                MATCH (u:User)
                OPTIONAL MATCH (u)-[r:HAS_PRIMARY_CRAFT]->()
                WITH u, count(r) as c
                WHERE c <> 1
                RETURN count(u) as invalid_users
            """).single()
            inv2 = res2["invalid_users"]
            if inv2 == 0:
                print("Check 2 PASS: Every User has exactly one HAS_PRIMARY_CRAFT edge.")
            else:
                print(f"Check 2 FAIL: {inv2} users do not have exactly one HAS_PRIMARY_CRAFT edge.")
                success = False

            # Check 3: Every User has exactly one LIVES_IN edge
            res3 = session.run("""
                MATCH (u:User)
                OPTIONAL MATCH (u)-[r:LIVES_IN]->()
                WITH u, count(r) as c
                WHERE c <> 1
                RETURN count(u) as invalid_users
            """).single()
            inv3 = res3["invalid_users"]
            if inv3 == 0:
                print("Check 3 PASS: Every User has exactly one LIVES_IN edge.")
            else:
                print(f"Check 3 FAIL: {inv3} users do not have exactly one LIVES_IN edge.")
                success = False

            # Check 4: No orphaned Project nodes
            res4 = session.run("""
                MATCH (p:Project)
                OPTIONAL MATCH ()-[r:CREDITED_ON|PRODUCED]->(p)
                WITH p, count(r) as c
                WHERE c = 0
                RETURN count(p) as orphaned
            """).single()
            inv4 = res4["orphaned"]
            if inv4 == 0:
                print("Check 4 PASS: No orphaned Project nodes.")
            else:
                print(f"Check 4 FAIL: Found {inv4} orphaned Project nodes.")
                success = False

            # Check 5: COLLABORATED_WITH edges should be symmetric
            res5 = session.run("""
                MATCH (a:User)-[:COLLABORATED_WITH]->(b:User)
                WHERE NOT (b)-[:COLLABORATED_WITH]->(a)
                RETURN count(a) as asymmetric
            """).single()
            inv5 = res5["asymmetric"]
            if inv5 == 0:
                print("Check 5 PASS: All COLLABORATED_WITH edges are symmetric.")
            else:
                print(f"Check 5 FAIL: Found {inv5} asymmetric COLLABORATED_WITH edges.")
                success = False

            # Check 6: Run a sample query
            res6 = session.run("""
                MATCH (u:User)-[:HAS_PRIMARY_CRAFT]->(c:Craft)
                MATCH (u)-[:LIVES_IN]->(l:Location)
                WHERE l.name =~ '(?i)hyderabad' AND (c.name =~ '(?i)director' OR c.name =~ '(?i).*director.*')
                AND u.verification_level = 'peer_verified'
                RETURN count(u) as count
            """).single()
            count6 = res6["count"]
            if count6 > 0:
                print(f"Check 6 PASS: Found {count6} directors in Hyderabad with verification_level = peer_verified.")
            else:
                print(f"Check 6 FAIL: Found 0 directors matching the criteria.")
                success = False

    finally:
        driver.close()

    if success:
        print("\nAll sanity checks passed! The indexing pipeline has no bugs.")
    else:
        print("\nSome sanity checks failed! Check the output above.")

if __name__ == "__main__":
    run_checks()
