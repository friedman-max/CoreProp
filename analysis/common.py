"""
Shared helpers for calibration disparity analysis.

All probes read from market_observatory (the unbiased, non-selection-filtered
event stream) UNLESS noted otherwise. Use legs+slips only when the question
specifically requires the user's logged-bets selection bias as a covariate.
"""
import os
import sys
import logging
from typing import Optional, Iterable

import pandas as pd
from dotenv import load_dotenv

# Quiet noisy supabase chatter so analysis output stays readable.
logging.getLogger("postgrest").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Make engine.database importable from a subdir script.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from engine.database import get_db  # noqa: E402

PAGE = 1000


def fetch_all(table: str, select: str, *, filters: Optional[dict] = None,
              order_col: str = "created_at", order_desc: bool = True,
              max_rows: int = 200_000) -> pd.DataFrame:
    """Page through a Supabase table into a DataFrame.

    `filters` is a dict like {"result": ("neq", "pending"), "league": ("in", "(NBA,WNBA,MLB,NHL)")}
    """
    db = get_db()
    if db is None:
        raise RuntimeError("No Supabase client — check .env for SUPABASE_URL + SUPABASE_SERVICE_KEY")

    rows = []
    offset = 0
    while offset < max_rows:
        q = db.table(table).select(select)
        if filters:
            for col, (op, val) in filters.items():
                # postgrest-py names: `in` is a reserved word, so the method
                # is `in_`. Same for `not_` (etc). Translate common shorthand.
                method = {"in": "in_", "not": "not_"}.get(op, op)
                getattr(q, method)(col, val)
        try:
            q = q.order(order_col, desc=order_desc)
        except Exception:
            pass
        res = q.range(offset, offset + PAGE - 1).execute()
        chunk = res.data or []
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < PAGE:
            break
        offset += PAGE
    return pd.DataFrame(rows)


def y_from_result(result: str) -> Optional[int]:
    """Mirror of engine/isotonic_calibration.py's outcome decoding."""
    if result is None:
        return None
    r = str(result).lower()
    if r in ("hit", "won", "win"):
        return 1
    if r in ("miss", "lost", "loss"):
        return 0
    return None  # push, dnp, pending


def attach_y(df: pd.DataFrame, result_col: str = "result") -> pd.DataFrame:
    df = df.copy()
    df["y"] = df[result_col].map(y_from_result)
    return df


def settled(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows without a binary outcome (push/dnp/pending)."""
    return df[df["y"].notna()].copy()


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple:
    """Wilson 95% CI for a binomial rate. Returns (low, high)."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    denom = 1 + z**2 / n
    center = p + z**2 / (2 * n)
    margin = z * ((p * (1 - p) / n + z**2 / (4 * n**2)) ** 0.5)
    return ((center - margin) / denom, (center + margin) / denom)


def bucket_prob(p: float, width: float = 0.05) -> float:
    """Round a probability into a fixed-width bucket center (e.g. 0.05 → 0.575 → 0.575)."""
    if p is None or pd.isna(p):
        return float("nan")
    lo = int(p / width) * width
    return round(lo + width / 2, 4)


def beta_binomial_posterior(k: int, n: int, prior_mean: float, prior_strength: float = 30.0) -> tuple:
    """Beta-Binomial posterior mean + 95% credible interval.

    Returns (posterior_mean, low, high).
    """
    alpha0 = prior_mean * prior_strength
    beta0 = (1 - prior_mean) * prior_strength
    alpha_post = alpha0 + k
    beta_post = beta0 + (n - k)
    mean = alpha_post / (alpha_post + beta_post)
    # Beta variance → normal approx for the 95% CI (good enough at n+prior_strength ≥ 30)
    var = (alpha_post * beta_post) / ((alpha_post + beta_post) ** 2 * (alpha_post + beta_post + 1))
    sd = var ** 0.5
    return (mean, max(0.0, mean - 1.96 * sd), min(1.0, mean + 1.96 * sd))


def brier_score(p: Iterable[float], y: Iterable[int]) -> float:
    """Mean (p - y)^2 over paired predictions/outcomes."""
    pp = pd.Series(p).astype(float)
    yy = pd.Series(y).astype(float)
    mask = pp.notna() & yy.notna()
    if not mask.any():
        return float("nan")
    return float(((pp[mask] - yy[mask]) ** 2).mean())


def pretty_print_table(df: pd.DataFrame, title: str = "") -> None:
    if title:
        print(f"\n=== {title} ===")
    with pd.option_context("display.max_rows", 200, "display.width", 220,
                            "display.max_columns", 50, "display.float_format", "{:.4f}".format):
        print(df.to_string(index=False))
