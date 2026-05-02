import math

LAMBDA_TIER = 0.5
MU_RECENCY = 0.15

ROLE_WEIGHTS = {
    "director": 1.0, "lead actor": 1.0, "dop": 1.0, "music director": 1.0, "casting director": 1.0, "producer": 1.0, "writer": 1.0,
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
