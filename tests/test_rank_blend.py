import pytest

from lib.math.constants import FINAL_SCORE_CCS_WEIGHT, FINAL_SCORE_KEYWORD_WEIGHT


def test_final_score_blend_midpoint():
    nc = nk = 1.0
    final_score = FINAL_SCORE_CCS_WEIGHT * nc + FINAL_SCORE_KEYWORD_WEIGHT * nk
    assert final_score == pytest.approx(1.0)
