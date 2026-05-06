def fetch_candidates_context(session, user_ids):
    """
    Two round-trips: static profile context (banners, locations, mentors, verification)
    and per-credit verifier aggregation for MULTIPLE candidates.
    """
    if not user_ids:
        return {}

    ctx_res = session.run(
        """
        MATCH (u:User) WHERE u.id IN $uids
        OPTIONAL MATCH (u)-[:AFFILIATED_WITH]->(ab:Banner)
        OPTIONAL MATCH (u)-[:CREDITED_ON]->(_p:Project)<-[:PRODUCED]-(cb:Banner)
        OPTIONAL MATCH (u)-[:LIVES_IN|WILLING_TO_TRAVEL_TO]->(loc:Location)
        OPTIONAL MATCH (u)-[:TRAINED_UNDER]->(mentor:User)
        RETURN u.id AS uid,
               u.verification_level AS verification_level,
               u.experience_years AS experience_years,
               collect(DISTINCT ab.name) AS affiliated_banners,
               collect(DISTINCT cb.name) AS credited_banners,
               collect(DISTINCT loc.name) AS locations,
               collect(DISTINCT mentor.name) AS mentor_names
        """,
        uids=user_ids,
    )

    ctx_map = {}
    for row in ctx_res:
        uid = row["uid"]
        ctx_map[uid] = {
            "verification_level": row["verification_level"],
            "experience_years": row["experience_years"],
            "affiliated_banners": [x for x in (row["affiliated_banners"] or []) if x],
            "credited_banners": [x for x in (row["credited_banners"] or []) if x],
            "locations": [x for x in (row["locations"] or []) if x],
            "mentor_names": [x for x in (row["mentor_names"] or []) if x],
        }

    credits_res = session.run(
        """
        MATCH (u:User)-[r:CREDITED_ON]->(p:Project) WHERE u.id IN $uids
        OPTIONAL MATCH (p)<-[:CREDITED_ON]-(v:User) WHERE v.id <> u.id
        OPTIONAL MATCH (v)-[collab:COLLABORATED_WITH]->(v2:User)-[:CREDITED_ON]->(p)
        WITH u.id AS uid, p, r, v, count(collab) AS density
        WITH uid, p, r, collect({id: v.id, density: density, prior: 0.8}) AS verifiers
        RETURN uid, p.title AS title, p.year AS year, p.tier AS tier, p.type AS project_type,
               r.role AS role, verifiers
        """,
        uids=user_ids,
    )

    credits_map = {}
    for record in credits_res:
        uid = record["uid"]
        if uid not in credits_map:
            credits_map[uid] = []
        
        verifiers_raw = record["verifiers"]
        verifiers = [x for x in verifiers_raw if x.get("id")]
        credits_map[uid].append(
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

    result_map = {}
    for uid in user_ids:
        c = ctx_map.get(uid, {})
        result_map[uid] = {
            "verification_level": c.get("verification_level"),
            "experience_years": c.get("experience_years"),
            "context": {
                "affiliated_banners": c.get("affiliated_banners", []),
                "credited_banners": c.get("credited_banners", []),
                "locations": c.get("locations", []),
                "mentor_names": c.get("mentor_names", []),
            },
            "credits": credits_map.get(uid, []),
        }
        
    return result_map

def fetch_candidate_context(session, user_id):
    """Legacy wrapper for single candidate fetch"""
    return fetch_candidates_context(session, [user_id]).get(user_id, {})
