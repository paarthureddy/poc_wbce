from lib.pipeline.normalize_query import (
    detect_relationship_hint,
    normalize_query_params,
    parse_height_from_text,
)


def test_detect_relationship_hint_brother():
    assert detect_relationship_hint("need a brother actor") == "brother"


def test_parse_height_from_text_cm():
    hr = parse_height_from_text("need actor 176 cm")
    assert hr is not None
    assert 170 <= hr.min_cm <= 176
    assert 176 <= hr.max_cm <= 179


def test_parse_height_from_text_feet_inches():
    hr = parse_height_from_text("need actor 5'10")
    assert hr is not None
    # 5'10 ≈ 178cm
    assert hr.min_cm <= 178 <= hr.max_cm


def test_normalize_query_params_sets_height_bounds():
    out = normalize_query_params("actor 6ft", {"craft": "actor"})
    assert out["height_min_cm"] is not None
    assert out["height_max_cm"] is not None
    assert out["height_min_cm"] < out["height_max_cm"]

