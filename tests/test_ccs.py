import pytest

from lib.math.ccs import compute_ccs


def _credit(**kwargs):
    defaults = {
        "project_id": "p1",
        "title": "T",
        "year": 2024,
        "tier": 3,
        "role": "Director",
        "verifiers": [{"id": "v1", "density": 1.0, "prior": 0.8}],
    }
    defaults.update(kwargs)
    return defaults


def test_compute_ccs_empty_credits():
    out = compute_ccs(
        {
            "verification_level": "peer_verified",
            "context": {
                "affiliated_banners": [],
                "credited_banners": [],
                "locations": [],
                "mentor_names": [],
            },
            "credits": [],
        },
        {"tier": 3, "_query_tokens": []},
    )
    assert out["total"] == 0.0
    assert out["raw_ccs_total"] == 0.0


def test_compute_ccs_senior_outranks_newcomer_same_gamma():
    ctx = {"affiliated_banners": [], "credited_banners": [], "locations": []}
    senior = {
        "verification_level": "platform_verified",
        "context": ctx,
        "credits": [
            _credit(year=2024, tier=3, role="Director"),
            _credit(year=2023, tier=3, role="Director"),
            _credit(year=2022, tier=3, role="Director"),
            _credit(year=2021, tier=3, role="Director"),
            _credit(year=2020, tier=3, role="Director"),
        ],
    }
    newcomer = {
        "verification_level": "self_attested",
        "context": ctx,
        "credits": [_credit(year=2024, tier=3, role="Director", verifiers=[])],
    }
    q = {"tier": 3, "_query_tokens": []}
    s = compute_ccs(senior, q)
    n = compute_ccs(newcomer, q)
    assert s["total"] > n["total"]


def test_verification_multiplier_applied():
    base = {
        "verification_level": "self_attested",
        "context": {"affiliated_banners": [], "credited_banners": [], "locations": []},
        "credits": [_credit()],
    }
    boosted = {**base, "verification_level": "platform_verified"}
    q = {"tier": None, "_query_tokens": []}
    b = compute_ccs(boosted, q)
    s = compute_ccs(base, q)
    assert b["raw_ccs_total"] == pytest.approx(s["raw_ccs_total"])
    assert b["total"] > s["total"]
