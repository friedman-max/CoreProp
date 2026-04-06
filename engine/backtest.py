"""
Backtest and slip logger for CoreProp.

Automatically documents the best +EV slip combinations as they appear
throughout the day. Logs to data/backtest.csv with one row per leg.
Tracks which (player, prop, side) combos have been used to avoid repeats.
Resets daily at midnight.
"""
import csv
import logging
import pathlib
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from engine.constants import BREAK_EVEN
from engine.ev_calculator import power_slip_ev, flex_slip_ev

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH  = DATA_DIR / "backtest.csv"

CSV_COLUMNS = [
    "slip_id", "timestamp", "slip_type", "n_legs", "proj_slip_ev_pct",
    "leg_num", "player", "league", "prop", "line", "side",
    "true_prob", "ind_ev_pct", "urgency", "game_start",
    "result", "stat_actual",
]

# Minutes before game start to treat as "HIGH" urgency
URGENCY_MINUTES = 60

# Extra score added when urgency is HIGH (same units as ind_ev_pct)
URGENCY_BONUS = 0.02


class BacktestLogger:
    """
    Builds and logs the best available +EV slips to a CSV file.

    Selection logic:
      - Filter out already-used (player, prop, side) combos
      - Score each bet: score = ind_ev_pct + URGENCY_BONUS if game within 60 min
      - Try slip sizes 6 → 5 → 4 → 3, pick first size where best_ev > 0
        and average true_prob meets the break-even threshold
      - Log to CSV; mark used bets so they won't appear in future slips today
    """

    def __init__(self):
        self.used_bets: set[tuple] = set()  # (player_name_lower, prop_type_lower, side)
        self.last_reset_date: Optional[date] = None
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._init_csv()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _init_csv(self) -> None:
        """Create CSV with headers if it doesn't already exist."""
        if not CSV_PATH.exists():
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            logger.info("Created backtest CSV at %s", CSV_PATH)

    def _midnight_reset(self) -> None:
        """Automatically reset the used-bets pool when the calendar date changes."""
        today = date.today()
        if self.last_reset_date != today:
            if self.last_reset_date is not None:
                logger.info(
                    "Midnight reset: clearing %d used-bet keys", len(self.used_bets)
                )
            self.used_bets = set()
            self.last_reset_date = today

    def reset_daily(self) -> None:
        """Explicit daily reset — called by the APScheduler midnight job."""
        logger.info("Daily reset: clearing %d used-bet keys", len(self.used_bets))
        self.used_bets = set()
        self.last_reset_date = date.today()

    # ------------------------------------------------------------------
    # Scoring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_urgent(game_start: Optional[str]) -> bool:
        """Return True if the game starts within URGENCY_MINUTES from now."""
        if not game_start:
            return False
        try:
            gs = datetime.fromisoformat(game_start.replace("Z", "+00:00"))
            if gs.tzinfo is None:
                now = datetime.utcnow()
                gs = gs.replace(tzinfo=None)
            else:
                now = datetime.now(tz=timezone.utc)
            minutes_to_start = (gs - now).total_seconds() / 60
            return 0 < minutes_to_start <= URGENCY_MINUTES
        except Exception:
            return False

    @classmethod
    def _score(cls, bet: dict) -> float:
        ev = float(bet.get("individual_ev_pct") or 0.0)
        bonus = URGENCY_BONUS if cls._is_urgent(bet.get("start_time")) else 0.0
        return ev + bonus

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def try_log_slip(self, bets: list[dict]) -> Optional[dict]:
        """
        Given the current list of +EV bet dicts (each must include
        player_name, prop_type, side, true_prob, individual_ev_pct,
        pp_line, league, start_time), try to build and log a valid slip.

        Returns a slip summary dict if a slip was logged, else None.
        """
        self._midnight_reset()

        # 1. Remove bets whose (player, prop, side) combo was already used today
        available = [
            b for b in bets
            if (
                b.get("player_name", "").lower(),
                b.get("prop_type", "").lower(),
                b.get("side", ""),
            ) not in self.used_bets
        ]

        if len(available) < 3:
            logger.debug(
                "Backtest: only %d unused bets available — skipping", len(available)
            )
            return None

        # 2. Score (EV + urgency bonus) and sort descending
        available.sort(key=lambda b: self._score(b), reverse=True)

        # 3. Try slip sizes from 6 down to 3
        for k in (6, 5, 4, 3):
            if len(available) < k:
                continue

            candidates  = available[:k]
            true_probs  = [float(c.get("true_prob") or 0.0) for c in candidates]
            avg_prob    = sum(true_probs) / k

            # Check break-even thresholds
            power_be = BREAK_EVEN.get((str(k), "power"))
            flex_be  = BREAK_EVEN.get((str(k), "flex"))

            power_ev: Optional[float] = None
            flex_ev:  Optional[float] = None

            if power_be is not None and avg_prob >= power_be:
                power_ev = power_slip_ev(true_probs)

            if flex_be is not None and avg_prob >= flex_be:
                flex_ev = flex_slip_ev(true_probs)

            # Pick best type
            best_type: Optional[str] = None
            best_ev:   Optional[float] = None

            if power_ev is not None and flex_ev is not None:
                if power_ev >= flex_ev:
                    best_type, best_ev = "Power", power_ev
                else:
                    best_type, best_ev = "Flex", flex_ev
            elif power_ev is not None and power_ev > 0:
                best_type, best_ev = "Power", power_ev
            elif flex_ev is not None and flex_ev > 0:
                best_type, best_ev = "Flex", flex_ev

            if best_ev is None or best_ev <= 0:
                logger.debug(
                    "Backtest: %d-leg slip EV=%.4f not positive — trying smaller",
                    k, best_ev or 0,
                )
                continue

            # 4. Build CSV rows and log the slip
            slip_id   = str(uuid.uuid4())[:8].upper()
            timestamp = datetime.now().isoformat(timespec="seconds")
            proj_ev   = round(best_ev, 4)

            rows = []
            for i, bet in enumerate(candidates, start=1):
                urgency = "HIGH" if self._is_urgent(bet.get("start_time")) else "NORMAL"
                rows.append({
                    "slip_id":          slip_id,
                    "timestamp":        timestamp,
                    "slip_type":        best_type,
                    "n_legs":           k,
                    "proj_slip_ev_pct": proj_ev,
                    "leg_num":          i,
                    "player":           bet.get("player_name", ""),
                    "league":           bet.get("league", ""),
                    "prop":             bet.get("prop_type", ""),
                    "line":             bet.get("pp_line", ""),
                    "side":             bet.get("side", ""),
                    "true_prob":        round(float(bet.get("true_prob") or 0), 4),
                    "ind_ev_pct":       round(float(bet.get("individual_ev_pct") or 0), 4),
                    "urgency":          urgency,
                    "game_start":       bet.get("start_time", ""),
                    "result":           "pending",
                    "stat_actual":      "",
                })

            try:
                with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
                    csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerows(rows)
                logger.info(
                    "Backtest: logged slip %s  (%d-leg %s  EV=%.2f%%)",
                    slip_id, k, best_type, best_ev * 100,
                )
            except Exception as exc:
                logger.error("Backtest: CSV write failed: %s", exc)
                return None

            # 5. Mark legs as used
            for bet in candidates:
                key = (
                    bet.get("player_name", "").lower(),
                    bet.get("prop_type", "").lower(),
                    bet.get("side", ""),
                )
                self.used_bets.add(key)

            # 6. Return slip summary for the frontend notification
            return {
                "slip_id":          slip_id,
                "timestamp":        timestamp,
                "slip_type":        best_type,
                "n_legs":           k,
                "proj_slip_ev_pct": proj_ev,
                "legs": [
                    {
                        "player":     r["player"],
                        "league":     r["league"],
                        "prop":       r["prop"],
                        "line":       r["line"],
                        "side":       r["side"],
                        "true_prob":  r["true_prob"],
                        "ind_ev_pct": r["ind_ev_pct"],
                        "urgency":    r["urgency"],
                        "game_start": r["game_start"],
                    }
                    for r in rows
                ],
            }

        logger.debug(
            "Backtest: no valid slip found from %d available bets", len(available)
        )
        return None
