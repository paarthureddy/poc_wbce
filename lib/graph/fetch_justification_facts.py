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
               u.height_cm AS height_cm,
               u.build AS build,
               u.appearance_tags AS appearance_tags,
               u.languages_spoken AS languages_spoken,
               u.location_city AS location_city,
               u.location_state AS location_state,
               u.location_country AS location_country,
               u.regional_background AS regional_background,
               collect(DISTINCT pc.name)[0] AS primary_craft,
               count(DISTINCT e) AS peer_endorsement_count
        """,
        uid=user_id,
    ).single()

    if not row:
        return {}

    def _as_list(v):
        if isinstance(v, list):
            return [x for x in v if x is not None]
        return []

    return {
        "name": row["name"],
        "age": row["age"],
        "bio": row["bio"],
        "gender": row["gender"],
        "experience_years": row["experience_years"],
        "verification_level": row["verification_level"],
        "looking_for": _as_list(row["looking_for"]),
        "tags_self": _as_list(row["tags_self"]),
        "height_cm": row["height_cm"],
        "build": row["build"],
        "appearance_tags": _as_list(row["appearance_tags"]),
        "languages_spoken": _as_list(row["languages_spoken"]),
        "location_city": row["location_city"],
        "location_state": row["location_state"],
        "location_country": row["location_country"],
        "regional_background": row["regional_background"],
        "primary_craft": row["primary_craft"],
        "peer_endorsement_count": int(row["peer_endorsement_count"] or 0),
    }
