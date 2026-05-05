import os

from dotenv import load_dotenv
from neo4j import GraphDatabase


def main() -> None:
    load_dotenv()
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd = os.getenv("NEO4J_PASSWORD", "password")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    try:
        with driver.session() as session:
            def q(cypher: str):
                return [r.data() for r in session.run(cypher)]

            print("Lucknow users (city property):")
            print(
                q(
                    """
                    MATCH (u:User)
                    WHERE toLower(coalesce(u.location_city,'')) CONTAINS 'lucknow'
                    RETURN u.id AS id, u.name AS name, u.location_city AS city, u.location_state AS state
                    LIMIT 20
                    """
                )
            )

            print("Lucknow users (LIVES_IN edge):")
            print(
                q(
                    """
                    MATCH (u:User)-[:LIVES_IN]->(l:Location)
                    WHERE toLower(l.name) CONTAINS 'lucknow'
                    RETURN u.id AS id, u.name AS name, l.name AS l_city, u.location_state AS state
                    LIMIT 20
                    """
                )
            )

            print("Users with state Uttar Pradesh (count):")
            print(
                q(
                    """
                    MATCH (u:User)
                    WHERE toLower(coalesce(u.location_state,'')) CONTAINS 'uttar pradesh'
                    RETURN count(u) AS n
                    """
                )
            )

            print("Sample UP users:")
            print(
                q(
                    """
                    MATCH (u:User)
                    WHERE toLower(coalesce(u.location_state,'')) CONTAINS 'uttar pradesh'
                    RETURN u.id AS id, u.name AS name, u.location_city AS city, u.location_state AS state
                    LIMIT 20
                    """
                )
            )
    finally:
        driver.close()


if __name__ == "__main__":
    main()

