from lib.pipeline.justify import (
    build_justification_evidence,
    template_justification,
    validate_justification,
    _as_plain_craft,
    _compute_query_alignment,
)


def _sample_ranked_row():
    return {
        "id": "user_test",
        "Name": "Test User",
        "Age": 40,
        "Bio": "Bio here",
        "Craft": "Director",
        "Region": "Hyderabad",
        "keyword_score": 2,
        "ccs_breakdown": [
            {
                "project_title": "Alpha",
                "phi": 1.0,
                "h": 1.0,
                "v": 0.6,
                "delta": 0.9,
                "gamma": 0.5,
                "contribution": 0.27,
            }
        ],
    }


def _sample_candidate_data():
    return {
        "verification_level": "peer_verified",
        "experience_years": 15,
        "context": {
            "affiliated_banners": ["Mythri Movie Makers"],
            "credited_banners": [],
            "locations": ["Hyderabad"],
            "mentor_names": ["S.S. Rajamouli"],
        },
        "credits": [
            {
                "title": "Alpha",
                "year": 2022,
                "tier": 3,
                "role": "Director",
                "project_type": "feature_film",
                "verifiers": [{"id": "v1", "density": 1.0, "prior": 0.8}],
            }
        ],
    }


def _sample_profile_facts():
    return {
        "name": "Test User",
        "age": 40,
        "bio": "Bio",
        "gender": "male",
        "experience_years": 15,
        "verification_level": "peer_verified",
        "looking_for": [],
        "tags_self": ["mass_entertainer"],
        "primary_craft": "Director",
        "peer_endorsement_count": 3,
        "height_cm": 180,
        "build": "athletic",
        "appearance_tags": [],
        "languages_spoken": ["Telugu"],
        "location_city": "Hyderabad",
        "location_state": "Telangana",
        "location_country": "India",
        "regional_background": "Telugu",
    }


def test_build_evidence_has_core_keys():
    qc = {"raw_query": "director in hyderabad", "craft": "director", "tier": 3}
    ev = build_justification_evidence(
        _sample_ranked_row(),
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    assert "structured_query" in ev
    assert "query_alignment" in ev
    assert ev["structured_query"]["craft"] == "director"
    assert ev["credit_summary"]["total_graph_credits"] == 1
    assert ev["ccs_credit_breakdown_top"][0]["title"] == "Alpha"


def test_template_passes_validation():
    qc = {"raw_query": "x", "craft": "director", "keywords": ["mass"]}
    ev = build_justification_evidence(
        _sample_ranked_row(),
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    t = template_justification(ev)
    assert validate_justification(t, ev)


def test_validate_rejects_comparative():
    ev = {"candidate": {}, "structured_query": {}, "query_alignment": []}
    bad = "Test User is a director. They are better than other candidates."
    assert not validate_justification(bad, ev)


def test_validate_rejects_decimal_scores():
    ev = {"candidate": {}, "structured_query": {}, "query_alignment": []}
    bad = "Strong fit with score 0.85 and verified credits."
    assert not validate_justification(bad, ev)


def test_validate_rejects_percentages():
    ev = {"candidate": {}, "structured_query": {}, "query_alignment": []}
    bad = "Matches 92% of query criteria."
    assert not validate_justification(bad, ev)


def test_validate_allows_plain_integers():
    """The new validator no longer whitelist-rejects integers — only decimals/percentages."""
    ev = {"candidate": {}, "structured_query": {}, "query_alignment": []}
    good = "5 credits, height 183 cm, most recent year 2023."
    assert validate_justification(good, ev)


def test_as_plain_craft_from_dict():
    assert _as_plain_craft({"craft": "Director", "subcraft": "X"}) == "Director"


def test_query_alignment_language_match():
    sq = {"language": "telugu"}
    facts = {"languages_spoken": ["Telugu", "English"]}
    align = _compute_query_alignment(sq, facts, "", [], [], [])
    lang = [a for a in align if a["dimension"] == "language"][0]
    assert lang["verdict"] == "matched"


def test_query_alignment_language_silent():
    sq = {"language": "telugu"}
    facts = {"languages_spoken": []}
    align = _compute_query_alignment(sq, facts, "", [], [], [])
    lang = [a for a in align if a["dimension"] == "language"][0]
    assert lang["verdict"] == "silent"


def test_query_alignment_tall_match_by_height():
    sq = {"keywords": ["tall"]}
    facts = {"height_cm": 185}
    align = _compute_query_alignment(sq, facts, "", [], [], [])
    tall = [a for a in align if a["dimension"] == "physical:tall"][0]
    assert tall["verdict"] == "matched"


def test_query_alignment_dark_match_via_appearance():
    sq = {"keywords": ["dark"]}
    facts = {"appearance_tags": ["dusky"]}
    align = _compute_query_alignment(sq, facts, "", [], [], [])
    dark = [a for a in align if a["dimension"] == "physical:complexion"][0]
    assert dark["verdict"] == "matched"


def test_query_alignment_silent_when_profile_missing_attribute():
    sq = {"keywords": ["dark"]}
    facts = {"appearance_tags": [], "bio": ""}
    align = _compute_query_alignment(sq, facts, "", [], [], [])
    dark = [a for a in align if a["dimension"] == "physical:complexion"][0]
    assert dark["verdict"] == "silent"


def test_template_renders_query_match_section():
    qc = {
        "raw_query": "tall dark telugu actor",
        "craft": "director",
        "language": "telugu",
        "keywords": ["tall"],
        "location_city": "Hyderabad",
    }
    ev = build_justification_evidence(
        _sample_ranked_row(),
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    t = template_justification(ev)
    assert "Query Match" in t
    assert "language" in t.lower()
