from lib.pipeline.cypher import build_cypher


def test_cypher_includes_state_location_matching():
    q = build_cypher({"location_state": "uttar pradesh"})
    assert "u.location_state" in q
    assert "WILLING_TO_TRAVEL_TO" in q


def test_cypher_includes_height_range():
    q = build_cypher({"height_min_cm": 178, "height_max_cm": 188})
    assert "u.height_cm >= 178" in q
    assert "u.height_cm <= 188" in q

