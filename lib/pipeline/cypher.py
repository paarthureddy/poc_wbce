"""Cypher generation for talent search (craft, location, banner, keywords, demographics)."""


def _lc(s):
    return str(s).strip().lower()


def _escape_cypher_string(s: str) -> str:
    # Minimal escaping for single-quoted cypher strings.
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def build_cypher(params):
    lines = ["MATCH (u:User)"]

    node_wheres = []
    if params.get("gender"):
        node_wheres.append(f"toLower(u.gender) = toLower('{params['gender']}')")
    if params.get("age_range"):
        if params["age_range"] == "young":
            node_wheres.append("u.age <= 30")
        elif params["age_range"] == "mid":
            node_wheres.append("u.age > 30 AND u.age <= 50")
        elif params["age_range"] == "senior":
            node_wheres.append("u.age > 50")

    if node_wheres:
        lines.append("WHERE " + " AND ".join(node_wheres))

    if params.get("craft"):
        craft = params["craft"]
        lines.append(
            f"MATCH (u)-[:HAS_PRIMARY_CRAFT]->(c:Craft) WHERE toLower(c.name) CONTAINS toLower('{craft}')"
        )

    # Location matching: use normalized state/city if present, otherwise legacy `location`.
    # Retrieval checks both profile properties (seeded) and graph edges for recall.
    loc_city = params.get("location_city")
    loc_state = params.get("location_state")
    loc_legacy = params.get("location")
    loc_aliases = params.get("location_aliases") or []
    loc_terms = []
    for v in [loc_city, loc_state, loc_legacy]:
        if v and isinstance(v, str):
            loc_terms.append(v)
    for a in loc_aliases:
        if a and isinstance(a, str):
            loc_terms.append(a)

    if loc_terms:
        loc_terms = [_escape_cypher_string(t) for t in loc_terms[:8]]
        ors = []
        for t in loc_terms:
            ors.append(f"(u.location_city IS NOT NULL AND toLower(u.location_city) CONTAINS toLower('{t}'))")
            ors.append(f"(u.location_state IS NOT NULL AND toLower(u.location_state) CONTAINS toLower('{t}'))")
            ors.append(f"(u.location_country IS NOT NULL AND toLower(u.location_country) CONTAINS toLower('{t}'))")
            ors.append(f"(l IS NOT NULL AND toLower(l.name) CONTAINS toLower('{t}'))")
            ors.append(f"(lt IS NOT NULL AND toLower(lt.name) CONTAINS toLower('{t}'))")

        lines.append("OPTIONAL MATCH (u)-[:LIVES_IN]->(l:Location)")
        lines.append("OPTIONAL MATCH (u)-[:WILLING_TO_TRAVEL_TO]->(lt:Location)")
        lines.append("WITH DISTINCT u, l, lt")
        lines.append("WHERE " + " OR ".join(ors))

    if params.get("banner"):
        banner = params["banner"]
        lines.append(
            f"MATCH (u)-[:CREDITED_ON]->(p_banner:Project)<-[:PRODUCED]-(b:Banner) WHERE toLower(b.name) CONTAINS toLower('{banner}')"
        )

    # Height range (cm) if present in params.
    hmin = params.get("height_min_cm")
    hmax = params.get("height_max_cm")
    try:
        hmin_i = int(hmin) if hmin is not None else None
        hmax_i = int(hmax) if hmax is not None else None
    except (TypeError, ValueError):
        hmin_i = None
        hmax_i = None
    if hmin_i is not None or hmax_i is not None:
        h_wheres = []
        if hmin_i is not None:
            h_wheres.append(f"u.height_cm >= {hmin_i}")
        if hmax_i is not None:
            h_wheres.append(f"u.height_cm <= {hmax_i}")
        if h_wheres:
            if any(line.startswith("WITH DISTINCT u, l, lt") for line in lines):
                # already in a WITH/WHERE context due to location
                lines.append("AND " + " AND ".join(h_wheres))
            else:
                lines.append("WHERE " + " AND ".join(h_wheres))

    STOP_WORDS = {
        "brother",
        "sister",
        "friend",
        "villain",
        "hero",
        "role",
        "character",
        "soonish",
        "available",
        "budget",
        "experienced",
        "good",
        "best",
        "need",
        "someone",
        "like",
        "prefer",
        "based",
        "looking",
        "want",
        "find",
    }

    physique_kws = (
        params.get("physique", [])
        if isinstance(params.get("physique"), list)
        else ([params.get("physique")] if params.get("physique") else [])
    )
    generic_kws = [k for k in (params.get("keywords") or []) if k.lower() not in STOP_WORDS]
    all_keywords = physique_kws + generic_kws

    if all_keywords:
        lines.append("WITH DISTINCT u")
        lines.append("OPTIONAL MATCH (u)-[:CREDITED_ON]->(p_keywords:Project)")
        lines.append("WITH u, collect(p_keywords.type) as project_types")

        score_cases = []
        for kw in all_keywords:
            kw_clean = kw.replace("'", "\\'")
            score_cases.append(
                f"(CASE WHEN toLower(u.bio) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)"
            )
            score_cases.append(
                f"(CASE WHEN any(tag IN u.tags_self WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)"
            )
            score_cases.append(
                f"(CASE WHEN any(tag IN u.appearance_tags WHERE toLower(tag) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)"
            )
            score_cases.append(
                f"(CASE WHEN toLower(u.build) CONTAINS toLower('{kw_clean}') THEN 1 ELSE 0 END)"
            )
            score_cases.append(
                f"(CASE WHEN any(ptype IN project_types WHERE toLower(ptype) CONTAINS toLower('{kw_clean}')) THEN 1 ELSE 0 END)"
            )

        lines.append("WITH u, (" + " + ".join(score_cases) + ") AS keyword_score")
        lines.append("OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(craft:Craft)")
        lines.append("OPTIONAL MATCH (u)-[:LIVES_IN]->(loc:Location)")
        lines.append(
            "RETURN DISTINCT u.id AS id, u.name AS Name, u.age AS Age, u.bio AS Bio, collect(DISTINCT craft.name)[0] AS Craft, collect(DISTINCT loc.name)[0] AS Region, keyword_score"
        )
        lines.append("ORDER BY keyword_score DESC")
    else:
        lines.append("OPTIONAL MATCH (u)-[:HAS_PRIMARY_CRAFT]->(craft:Craft)")
        lines.append("OPTIONAL MATCH (u)-[:LIVES_IN]->(loc:Location)")
        lines.append(
            "RETURN DISTINCT u.id AS id, u.name AS Name, u.age AS Age, u.bio AS Bio, collect(DISTINCT craft.name)[0] AS Craft, collect(DISTINCT loc.name)[0] AS Region, 0 AS keyword_score"
        )

    return "\n".join(lines)
