"""Green-devil (goblin) / demon payout correctness — recommendation #1.

Covers three guarantees:
  1. Standard-table fix: Power-6 pays 37.5x (PrizePicks lowered it from 40x),
     so the Python table, the derived break-even, and slip EV all reflect 37.5x.
  2. Goblin slips are scored with the conservative payout factor, and a goblin
     slip's EV/payout is NEVER credited above what the standard table would give
     (the rule: never make a goblin look more +EV than reality).
  3. odds_type round-trips onto the logged leg row so historical goblin slips
     are identifiable and re-scoreable.
"""
from __future__ import annotations

from engine.constants import (
    POWER_PAYOUTS, FLEX_PAYOUTS, BREAK_EVEN,
    ODDS_TYPE_PAYOUT_FACTOR, slip_payout_factor,
)
from engine.ev_calculator import power_slip_ev, flex_slip_ev


# ── 1. Standard-table fix ────────────────────────────────────────────────────

def test_power6_pays_37_5_not_40():
    assert POWER_PAYOUTS[6] == 37.5, "Power-6 must be PrizePicks' current 37.5x"
    # Other tiers unchanged.
    assert POWER_PAYOUTS[2] == 3.0 and POWER_PAYOUTS[5] == 20.0


def test_power6_break_even_tracks_37_5():
    # BE = (1/payout)^(1/n). At 37.5x the 6-Power break-even is ~0.5466 (was
    # 0.5407 at 40x). The table must match the payout it's derived from.
    expected = (1 / 37.5) ** (1 / 6)
    assert abs(BREAK_EVEN[("6", "power")] - expected) < 1e-3


def test_power6_ev_uses_37_5():
    probs = [0.60] * 6
    ev = power_slip_ev(probs)  # default factor 1.0 (standard)
    expected = (0.60 ** 6) * 37.5 - 1.0
    assert abs(ev - expected) < 1e-9


# ── 2. Goblin/demon factor, never overstated ─────────────────────────────────

def test_factor_standard_is_identity():
    assert slip_payout_factor(["standard"] * 6) == 1.0
    assert slip_payout_factor([]) == 1.0
    # Unknown / missing odds types are treated as standard (no adjustment).
    assert slip_payout_factor([None, "", "mystery"]) == 1.0


def test_goblin_factor_below_one():
    assert ODDS_TYPE_PAYOUT_FACTOR["goblin"] < 1.0
    assert slip_payout_factor(["goblin"]) == ODDS_TYPE_PAYOUT_FACTOR["goblin"]
    # Multiplicative across legs.
    f2 = slip_payout_factor(["goblin", "goblin"])
    assert abs(f2 - ODDS_TYPE_PAYOUT_FACTOR["goblin"] ** 2) < 1e-12


def test_goblin_slip_never_scored_above_standard():
    # For the SAME leg probabilities, a goblin slip's EV must be <= the standard
    # slip's EV (goblins pay less; we must never overstate them).
    probs = [0.62] * 6
    std_ev = power_slip_ev(probs, slip_payout_factor(["standard"] * 6))
    gob_ev = power_slip_ev(probs, slip_payout_factor(["goblin"] * 6))
    assert gob_ev <= std_ev
    # Flex too.
    fprobs = [0.60] * 5
    std_f = flex_slip_ev(fprobs, slip_payout_factor(["standard"] * 5))
    gob_f = flex_slip_ev(fprobs, slip_payout_factor(["goblin"] * 5))
    assert gob_f <= std_f


def test_demon_factor_above_one():
    # Demons pay more (harder line); defined for completeness even though they
    # aren't scraped/logged today.
    assert ODDS_TYPE_PAYOUT_FACTOR["demon"] > 1.0


# ── 3. odds_type persists onto the leg row ───────────────────────────────────

class _FakeQuery:
    def __init__(self, db, name):
        self.db, self.name, self._op, self._pending = db, name, None, None
    def select(self, *a, **k): return self
    def eq(self, *a, **k): return self
    def in_(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def insert(self, rows): self._op = "insert"; self._pending = rows if isinstance(rows, list) else [rows]; return self
    def delete(self, *a, **k): self._op = "delete"; return self
    def execute(self):
        class _R:
            def __init__(s, d): s.data = d
        tbl = self.db.slips if self.name == "slips" else self.db.legs
        if self._op == "insert":
            tbl.extend(self._pending)
        return _R([])


class _FakeDB:
    def __init__(self): self.slips, self.legs = [], []
    def table(self, name): return _FakeQuery(self, name)


def _goblin_bet(i):
    return {
        "player_name": f"P{i}", "prop_type": "Points", "pp_line": 18.5, "side": "over",
        "start_time": "2026-07-14T23:00:00Z", "team": ("WAS" if i % 2 else "NYL"),
        "league": "WNBA", "true_prob": 0.72, "raw_true_prob": 0.72,
        "individual_ev_pct": 0.10, "odds_type": "goblin",
    }


def test_odds_type_persists_on_logged_leg():
    from engine.backtest import BacktestLogger
    db = _FakeDB()
    bl = BacktestLogger(user_id="u1", db_client=db)
    # Goblins are high-prob; a Power-6 of them clears the gates and logs.
    result = bl.try_log_slip([_goblin_bet(i) for i in range(6)], slip_type="Power", n_legs=6)
    assert result is not None, "goblin slip should log"
    assert len(db.legs) == 6
    assert all(l.get("odds_type") == "goblin" for l in db.legs), \
        "every logged leg must carry its odds_type"


def test_standard_leg_defaults_to_standard_odds_type():
    from engine.backtest import BacktestLogger
    db = _FakeDB()
    bl = BacktestLogger(user_id="u1", db_client=db)
    bets = [dict(_goblin_bet(i), odds_type="standard", true_prob=0.66, raw_true_prob=0.66)
            for i in range(6)]
    result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)
    assert result is not None
    assert all(l.get("odds_type") == "standard" for l in db.legs)
