import math

LAMBDA_TIER = 0.5
MU_RECENCY = 0.15

# Final list ordering: blend CCS (credibility) with Cypher keyword relevance.
FINAL_SCORE_CCS_WEIGHT = 0.62
FINAL_SCORE_KEYWORD_WEIGHT = 0.38

# Soft floor when normalizing CCS across a candidate batch so zero-credit profiles
# (e.g. creators with strong keyword match) are not always dominated by tiny CCS noise.
CCS_NORMALIZATION_EPSILON = 0.22

# Experience boosts summed CCS before verification multiplier (bounded).
EXPERIENCE_BOOST_MAX_YEARS = 25
EXPERIENCE_BOOST_COEFF = 6.0

# User-level trust applied to summed CCS (before keyword blend).
VERIFICATION_MULTIPLIER = {
    "platform_verified": 1.2,
    "peer_verified": 1.0,
    "self_attested": 0.85,
}

# Γ camp: strong patronage vs weaker direct banner credit overlap.
CAMP_SCORE_AFFILIATED = 1.0
CAMP_SCORE_DIRECT_CREDIT = 0.6
CAMP_SCORE_QUERY_BANNER_NO_MATCH = 0.0
CAMP_SCORE_NEUTRAL = 0.5

# Lineage: keyword overlap with TRAINED_UNDER mentor name(s).
LINEAGE_SCORE_MATCH = 1.0
LINEAGE_SCORE_NEUTRAL = 0.5

ROLE_WEIGHTS = {
    "director": 1.0,
    "lead director": 1.0,
    "lead actor": 1.0,
    "dop": 1.0,
    "music director": 1.0,
    "casting director": 1.0,
    "producer": 1.0,
    "writer": 1.0,
    "co-director": 0.85, "supporting lead": 0.85, "associate dop": 0.85, "production controller": 0.9,
    "associate": 0.70, "associate director": 0.70, "second ad": 0.70, "assistant cinematographer": 0.70, "associate casting director": 0.70, "associate line producer": 0.70,
    "first ad": 0.75, "line producer": 0.85,
    "trainee": 0.40, "third ad": 0.40, "crew": 0.40,
    "uncredited": 0.15, "volunteer": 0.15
}

WEIGHTS_GAMMA = {
    "camp": 0.35,
    "lineage": 0.25,
    "kin": 0.15,
    "region": 0.25
}
