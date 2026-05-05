def fetch_candidate_context(session, user_id):
    """
    Two round-trips: static profile context (banners, locations, mentors, verification)
    and per-credit verifier aggregation. Keeps verifier logic readable.
    """
    ctx_row = session.run(
        """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[:AFFILIATED_WITH]->(ab:Banner)
        OPTIONAL MATCH (u)-[:CREDITED_ON]->(_p:Project)<-[:PRODUCED]-(cb:Banner)
        OPTIONAL MATCH (u)-[:LIVES_IN|WILLING_TO_TRAVEL_TO]->(loc:Location)
        OPTIONAL MATCH (u)-[:TRAINED_UNDER]->(mentor:User)
        RETURN u.verification_level AS verification_level,
               u.experience_years AS experience_years,
               collect(DISTINCT ab.name) AS affiliated_banners,
               collect(DISTINCT cb.name) AS credited_banners,
               collect(DISTINCT loc.name) AS locations,
               collect(DISTINCT mentor.name) AS mentor_names
        """,
        uid=user_id,
    ).single()

    verification_level = None
    affiliated_banners = []
    credited_banners = []
    locations = []
    mentor_names = []
    if ctx_row:
        verification_level = ctx_row["verification_level"]
        experience_years = ctx_row["experience_years"]
        affiliated_banners = [x for x in (ctx_row["affiliated_banners"] or []) if x]
        credited_banners = [x for x in (ctx_row["credited_banners"] or []) if x]
        locations = [x for x in (ctx_row["locations"] or []) if x]
        mentor_names = [x for x in (ctx_row["mentor_names"] or []) if x]

    credits_res = session.run(
        """
        MATCH (u:User {id: $uid})-[r:CREDITED_ON]->(p:Project)
        OPTIONAL MATCH (p)<-[:CREDITED_ON]-(v:User) WHERE v.id <> u.id
        OPTIONAL MATCH (v)-[collab:COLLABORATED_WITH]->(v2:User)-[:CREDITED_ON]->(p)
        WITH p, r, v, count(collab) AS density
        WITH p, r, collect({id: v.id, density: density, prior: 0.8}) AS verifiers
        RETURN p.title AS title, p.year AS year, p.tier AS tier, p.type AS project_type,
               r.role AS role, verifiers
        """,
        uid=user_id,
    )

    credits = []
    for record in credits_res:
        verifiers_raw = record["verifiers"]
        verifiers = [x for x in verifiers_raw if x.get("id")]
        credits.append(
            {
                "project_id": record["title"],
                "title": record["title"],
                "year": record["year"],
                "tier": record["tier"] if record["tier"] is not None else 3,
                "project_type": record.get("project_type"),
                "role": record["role"],
                "verifiers": verifiers,
            }
        )

    return {
        "verification_level": verification_level,
        "experience_years": experience_years,
        "context": {
            "affiliated_banners": affiliated_banners,
            "credited_banners": credited_banners,
            "locations": locations,
            "mentor_names": mentor_names,
        },
        "credits": credits,
    }
