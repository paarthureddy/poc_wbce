from lib.pipeline.justify import (
    build_justification_evidence,
    template_justification,
    validate_justification,
    _collect_allowed_number_strings,
    _as_plain_craft,
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
        "experience_years": 15,
        "verification_level": "peer_verified",
        "looking_for": [],
        "tags_self": ["mass_entertainer"],
        "primary_craft": "Director",
        "peer_endorsement_count": 3,
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
    qc = {"raw_query": "q"}
    ev = build_justification_evidence(
        _sample_ranked_row(),
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    bad = (
        "Test User is a director. They worked on Alpha in 2022. "
        "They are better than other candidates in this list."
    )
    assert not validate_justification(bad, ev)


def test_validate_rejects_unknown_number():
    qc = {"raw_query": "q"}
    ev = build_justification_evidence(
        _sample_ranked_row(),
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    bad = (
        "Test User is a director with 99 verified credits. "
        "They worked on Alpha in 2022. "
        "They are based in Hyderabad."
    )
    assert not validate_justification(bad, ev)


def test_allowed_numbers_include_query_digits():
    ev = {
        "candidate": {"age": 30},
        "structured_query": {},
        "credit_summary": {},
        "ccs_credit_breakdown_top": [],
        "query_numeric_tokens": ["22"],
        "keyword_relevance_score": None,
    }
    assert "22" in _collect_allowed_number_strings(ev)


def test_as_plain_craft_from_dict():
    assert _as_plain_craft({"craft": "Director", "subcraft": "X"}) == "Director"


def test_template_does_not_claim_keyword_alignment_when_score_zero():
    qc = {"raw_query": "action telangana telugu", "craft": "actor", "keywords": ["action", "telangana", "telugu"]}
    row = _sample_ranked_row()
    row["keyword_score"] = 0
    ev = build_justification_evidence(
        row,
        _sample_profile_facts(),
        _sample_candidate_data(),
        qc,
    )
    t = template_justification(ev).lower()
    assert "keywords action, telangana, telugu align" not in t
    assert "direct overlap" in t
