import math

import pytest

from lib.math.factors import phi, h, delta, v, gamma
from lib.math.constants import WEIGHTS_GAMMA


def test_phi_perfect_tier_match():
    assert phi(3, 3) == pytest.approx(1.0)


def test_phi_large_gap():
    assert phi(1, 5) == pytest.approx(math.exp(-0.5 * 4))


def test_phi_null_query_tier_neutral():
    assert phi(3, None) == 1.0
    assert phi(1, None) == 1.0


def test_h_known_roles():
    assert h("Director") == 1.0
    assert h("Trainee") == 0.4


def test_h_compound_role_picks_best():
    assert h("Writer and Director") == 1.0


def test_h_lead_director():
    assert h("Lead Director") == 1.0


def test_h_missing_neutral():
    assert h(None) == 0.5
    assert h("") == 0.5


def test_delta_this_year():
    assert delta(2026, 2026) == 1.0


def test_delta_ten_years():
    assert delta(2016, 2026) == pytest.approx(math.exp(-0.15 * 10))


def test_v_empty_neutral():
    assert v([]) == 0.5
    assert v(None) == 0.5


def test_v_one_verifier_above_neutral():
    assert v([{"density": 0.0, "prior": 0.8}]) > 0.5


def test_lineage_keyword_matches_mentor():
    qc = {"_query_tokens": ["rajamouli", "director"]}
    ctx = {
        "affiliated_banners": [],
        "credited_banners": [],
        "locations": [],
        "mentor_names": ["S.S. Rajamouli (Associate)"],
    }
    g_match = gamma(ctx, qc, WEIGHTS_GAMMA)
    g_none = gamma({**ctx, "mentor_names": []}, qc, WEIGHTS_GAMMA)
    assert g_match > g_none


def test_gamma_camp_affiliated_vs_direct():
    qc = {"banner": "mythri", "_query_tokens": []}
    w = WEIGHTS_GAMMA
    g1 = gamma(
        {
            "affiliated_banners": ["Mythri Movie Makers"],
            "credited_banners": [],
            "locations": [],
        },
        qc,
        w,
    )
    g2 = gamma(
        {
            "affiliated_banners": [],
            "credited_banners": ["Mythri Movie Makers"],
            "locations": [],
        },
        qc,
        w,
    )
    assert g1 > g2
