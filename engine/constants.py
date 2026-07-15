"""
Hardcoded PrizePicks break-even table and payout structures.
"""

# Per-leg break-even probability for each slip type, derived from the actual
# payout structures below. Power is closed-form: p = (1/payout)^(1/n_legs).
# Flex is solved numerically against the partial-payout schedule (the
# probability where E[payout] = 1).
BREAK_EVEN = {
    ("2", "power"): 0.5774,   # (1/3)^(1/2)
    ("3", "power"): 0.5503,   # (1/6)^(1/3)
    ("3", "flex"):  0.5774,
    ("4", "power"): 0.5623,   # (1/10)^(1/4)
    ("4", "flex"):  0.5503,
    ("5", "power"): 0.5493,   # (1/20)^(1/5)
    ("5", "flex"):  0.5425,
    ("6", "power"): 0.5466,   # (1/37.5)^(1/6) — PrizePicks lowered the 6-Power
                              # top payout from 40x to 37.5x; the break-even
                              # rises with it (was 0.5407 at 40x).
    ("6", "flex"):  0.5421,
}

# Power slip payout multipliers (decimal, e.g. 3x means you get 3x your stake back).
# Matches PrizePicks' current published Power Play table. NOTE: the 6-pick pays
# 37.5x today (PrizePicks reduced it from the older 40x); the frontend mirror in
# web/static/ev-page.jsx (EV_POWER_PAYOUTS) MUST stay identical to this table.
POWER_PAYOUTS = {
    2: 3.0,
    3: 6.0,
    4: 10.0,
    5: 20.0,
    6: 37.5,
}

# Flex payout tiers: {n_picks: {k_correct: decimal_multiplier}}
# Only tiers that pay out are listed; missing k → 0. Matches the current
# published Flex Play table; mirror EV_FLEX_PAYOUTS in ev-page.jsx.
FLEX_PAYOUTS = {
    3: {2: 1.0, 3: 3.0},
    4: {3: 1.5,  4: 6.0},
    5: {3: 0.4,  4: 2.0,  5: 10.0},
    6: {4: 0.4,  5: 2.0,  6: 25.0},
}

# The most efficient single-leg implied decimal odds (Power 6 / 1.849 multiplier)
# DEPRECATED for ranking / EV display: this is the per-leg break-even ONLY for
# a 6-leg Power slip. Legs near this threshold are EV-negative inside shorter
# slips (3-Power BE=0.5503, 4-Power BE=0.5623). Use `score_leg(p, n, type)`
# below for context-aware EV. The constants are kept so the existing
# strategy_tester / ev_calculator paths continue to compile.
OPTIMAL_IMPLIED_DECIMAL = 1.849
OPTIMAL_BREAK_EVEN = 1.0 / OPTIMAL_IMPLIED_DECIMAL  # ≈ 0.54083


def score_leg(
    p: float,
    slip_n: int = 6,
    slip_type: str = "power",
) -> float:
    """Context-aware per-leg EV%.

    Returns the leg's EV as a fraction of stake, given the slip-size and
    slip-type the leg will be deployed in. Replaces the legacy
    `p * OPTIMAL_IMPLIED_DECIMAL - 1` formula which silently assumed
    6-Power and misranked legs intended for 3-Power slips.

    Math:
      implied_decimal = 1 / BREAK_EVEN[(n, type)]
      ev_pct          = p * implied_decimal - 1
    """
    be = BREAK_EVEN.get((str(slip_n), slip_type.lower()), OPTIMAL_BREAK_EVEN)
    if be <= 0:
        return 0.0
    return p / be - 1.0


def per_leg_break_even(slip_n: int = 6, slip_type: str = "power") -> float:
    """Lookup helper. Returns OPTIMAL_BREAK_EVEN as the legacy default for
    callers that pass nothing."""
    return BREAK_EVEN.get((str(slip_n), slip_type.lower()), OPTIMAL_BREAK_EVEN)

# Leagues that must NEVER surface in any user-facing view, even if old rows
# (observatory, calibration JSON, correlation map, sharpness state) still
# contain them. Filter at every read path so legacy data can't leak back
# into the UI.
#
# Adding a league here is the single source of truth — `is_excluded_league`
# below and the per-module filters that consult it.
EXCLUDED_LEAGUES = frozenset()


def is_excluded_league(league: str | None) -> bool:
    """True if `league` belongs to the EXCLUDED_LEAGUES set (case-insensitive)."""
    if not league:
        return False
    return league.strip().upper() in EXCLUDED_LEAGUES

# Prop type normalization: FanDuel label → PrizePicks stat_type label
# Keys are league names, values are nested maps of {FanDuel label: PrizePicks label}
PROP_TYPE_MAP = {
    "NBA": {
        "points":                        "Points",
        "player points":                 "Points",
        "rebounds":                      "Rebounds",
        "player rebounds":               "Rebounds",
        "assists":                       "Assists",
        "player assists":                "Assists",
        "3-point field goals made":      "3-PT Made",
        "3 point field goals made":      "3-PT Made",
        "3-pointers made":               "3-PT Made",
        "three point field goals made":  "3-PT Made",
        "made threes":                   "3-PT Made",
        "steals":                        "Steals",
        "blocks":                        "Blocked Shots",
        "blocked shots":                 "Blocked Shots",
        "turnovers":                     "Turnovers",
        "pts + reb + ast":               "Pts+Rebs+Asts",
        "pts+reb+ast":                   "Pts+Rebs+Asts",
        "points + rebounds + assists":   "Pts+Rebs+Asts",
        "rebounds + assists":            "Rebs+Asts",
        "points + rebounds":             "Pts+Rebs",
        "points + assists":              "Pts+Asts",
        "blocks + steals":               "Blks+Stls",
        "steals + blocks":               "Blks+Stls",
        "fantasy score":                 "Fantasy Score",
        "offensive rebounds":            "Offensive Rebounds",
        "defensive rebounds":            "Defensive Rebounds",
        "field goals made":              "FG Made",
        "field goals attempted":         "FG Attempted",
        "free throws made":              "Free Throws Made",
        "free throws attempted":         "Free Throws Attempted",
        "personal fouls":                "Personal Fouls",
        "double-double":                 "Double-Double",
        "triple-double":                 "Triple-Double",
        "first basket":                  "First Basket",
    },
    "NCAAB": {
        "points":                        "Points",
        "rebounds":                      "Rebounds",
        "assists":                       "Assists",
        "3-point field goals made":      "3-PT Made",
        "three point field goals made":  "3-PT Made",
        "pts + reb + ast":               "Pts+Rebs+Asts",
    },
    "WNBA": {
        "points":                        "Points",
        "player points":                 "Points",
        "rebounds":                      "Rebounds",
        "player rebounds":               "Rebounds",
        "assists":                       "Assists",
        "player assists":                "Assists",
        "3-point field goals made":      "3-PT Made",
        "3 point field goals made":      "3-PT Made",
        "3-pointers made":               "3-PT Made",
        "three point field goals made":  "3-PT Made",
        "made threes":                   "3-PT Made",
        "steals":                        "Steals",
        "blocks":                        "Blocked Shots",
        "blocked shots":                 "Blocked Shots",
        "turnovers":                     "Turnovers",
        "pts + reb + ast":               "Pts+Rebs+Asts",
        "pts+reb+ast":                   "Pts+Rebs+Asts",
        "points + rebounds + assists":   "Pts+Rebs+Asts",
        "rebounds + assists":            "Rebs+Asts",
        "points + rebounds":             "Pts+Rebs",
        "points + assists":              "Pts+Asts",
        "blocks + steals":               "Blks+Stls",
        "steals + blocks":               "Blks+Stls",
        "fantasy score":                 "Fantasy Score",
        "double-double":                 "Double-Double",
        "triple-double":                 "Triple-Double",
        "first basket":                  "First Basket",
    },
    "MLB": {
        "strikeouts":                    "Pitcher Strikeouts",
        "pitcher strikeouts":            "Pitcher Strikeouts",
        "strikeouts thrown":             "Pitcher Strikeouts",
        "hits":                          "Hits",
        "runs":                          "Runs",
        "rbis":                          "RBIs",
        "total bases":                   "Total Bases",
        "walks":                         "Walks",
        "earned runs allowed":           "Earned Runs Allowed",
        "hits allowed":                  "Hits Allowed",
        "hitter fantasy score":          "Hitter Fantasy Score",
        "pitching outs":                 "Pitching Outs",
        "outs recorded":                 "Pitching Outs",
        "batting runs":                  "Runs",
        "home runs":                     "Home Runs",
        "stolen bases":                  "Stolen Bases",
        "singles":                       "Singles",
        "doubles":                       "Doubles",
        "triples":                       "Triples",
        "extra base hits":               "Extra Base Hits",
        "hits + runs + rbis":            "Hits+Runs+RBIs",
        "hits+runs+rbis":                "Hits+Runs+RBIs",
        "runs + rbis":                   "Runs+RBIs",
        "runs+rbis":                     "Runs+RBIs",
        "hitter strikeouts":             "Hitter Strikeouts",
    },
    "NHL": {
        "shots on goal":                 "Shots on Goal",
        "goals":                         "Goals",
        "saves":                         "Saves",
        "total saves":                   "Saves",
        "total goals":                   "Goals",
        "points":                        "Points",
        "assists":                       "Assists",
        "time on ice":                   "Time On Ice",
        "shots":                         "Shots on Goal",
        "player total shots":            "Shots on Goal",
        "player total saves":            "Saves",
        "player total goals":            "Goals",
        "player total assists":          "Assists",
        "player total points":           "Points",
        "power play points":             "Power Play Points",
        "1st period goals":              "1st Period Goals",
    },
}
