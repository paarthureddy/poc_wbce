"""Extra User fields for Component 5 justifications (single round-trip)."""


def fetch_justification_facts(session, user_id: str) -> dict:
    row = session.run(
        """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(pc:Craft)
        OPTIONAL MATCH (:User)-[:GAVE_ENDORSEMENT]->(e:Endorsement)-[:ABOUT]->(u)
        RETURN u.name AS name,
               u.age AS age,
               u.bio AS bio,
               u.gender AS gender,
               u.experience_years AS experience_years,
               u.verification_level AS verification_level,
               u.looking_for AS looking_for,
               u.tags_self AS tags_self,
               collect(DISTINCT pc.name)[0] AS primary_craft,
               count(DISTINCT e) AS peer_endorsement_count
        """,
        uid=user_id,
    ).single()

    if not row:
        return {}

    lf = row["looking_for"] or []
    if not isinstance(lf, list):
        lf = []

    tags = row["tags_self"] or []
    if not isinstance(tags, list):
        tags = []

    return {
        "name": row["name"],
        "age": row["age"],
        "bio": row["bio"],
        "gender": row["gender"],
        "experience_years": row["experience_years"],
        "verification_level": row["verification_level"],
        "looking_for": lf,
        "tags_self": tags,
        "primary_craft": row["primary_craft"],
        "peer_endorsement_count": int(row["peer_endorsement_count"] or 0),
    }
