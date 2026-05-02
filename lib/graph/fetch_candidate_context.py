def fetch_candidate_context(session, user_id):
    # Fetch banners
    banners_res = session.run("""
        MATCH (u:User {id: $uid})-[:AFFILIATED_WITH]->(b:Banner)
        RETURN collect(b.name) as banners
    """, uid=user_id)
    banners = banners_res.single()["banners"] if banners_res.peek() else []

    # Fetch locations
    loc_res = session.run("""
        MATCH (u:User {id: $uid})-[:LIVES_IN|WILLING_TO_TRAVEL_TO]->(l:Location)
        RETURN collect(l.name) as locations
    """, uid=user_id)
    locations = loc_res.single()["locations"] if loc_res.peek() else []

    # Fetch credits with project info and verifiers
    credits_res = session.run("""
        MATCH (u:User {id: $uid})-[r:CREDITED_ON]->(p:Project)
        OPTIONAL MATCH (p)<-[:CREDITED_ON]-(v:User) WHERE v.id <> u.id
        OPTIONAL MATCH (v)-[collab:COLLABORATED_WITH]->(v2:User)-[:CREDITED_ON]->(p)
        WITH p, r, v, count(collab) as density
        WITH p, r, collect({id: v.id, density: density, prior: 0.8}) as verifiers
        RETURN p.title as title, p.year as year, p.tier as tier, r.role as role, verifiers
    """, uid=user_id)
    
    credits = []
    for record in credits_res:
        verifiers_raw = record["verifiers"]
        verifiers = [v for v in verifiers_raw if v.get("id")]
        credits.append({
            "project_id": record["title"],
            "title": record["title"],
            "year": record["year"],
            "tier": record["tier"] or 3,
            "role": record["role"],
            "verifiers": verifiers
        })

    return {
        "context": {
            "banners": banners,
            "locations": locations
        },
        "credits": credits
    }
