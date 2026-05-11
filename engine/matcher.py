"""
Fuzzy matching between FanDuel props and PrizePicks lines.
"""
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz, process

from engine.constants import PROP_TYPE_MAP
from config import FUZZY_THRESHOLD


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class FanDuelProp:
    league: str
    player_name: str
    prop_type: str       # normalized to PrizePicks label
    line: float
    over_odds: Optional[int]    # American; None if not available
    under_odds: Optional[int]   # American; None if not available
    both_sided: bool
    start_time: str = ""


@dataclass(slots=True)
class PrizePickLine:
    league: str
    player_name: str
    stat_type: str       # PrizePicks stat_type label
    line_score: float
    player_id: str       # PrizePicks internal ID
    start_time: str = "" # ISO timestamp of game start
    side: str = "both"   # "both", "over", or "under"
    team: str = ""       # PP team abbreviation (e.g. "MIN", "LAL")


@dataclass(slots=True)
class MatchedProp:
    pp: PrizePickLine
    name_score: float    # fuzzy similarity 0-100
    # Exact-line match per book (book.line == pp.line_score). Stays the
    # canonical handle for downstream code that doesn't care about
    # half-step equivalence.
    fd: Optional[FanDuelProp] = None
    dk: Optional[FanDuelProp] = None
    pin: Optional[FanDuelProp] = None
    # When pp.line_score is a whole number, "over PP-N" is mathematically
    # the same wager as "over book-(N+0.5)" under push-on-tie semantics
    # (player needs N+1+ to win in both); same for "under PP-N" vs "under
    # book-(N-0.5)". These per-side fields point to whichever book candidate
    # (same player, alt line) covers that side. For fractional PP lines they
    # are populated only when an exact-line match exists, so callers can
    # always read the per-side fields without branching on line parity.
    fd_over_equiv:    Optional[FanDuelProp] = None
    fd_under_equiv:   Optional[FanDuelProp] = None
    dk_over_equiv:    Optional[FanDuelProp] = None
    dk_under_equiv:   Optional[FanDuelProp] = None
    pin_over_equiv:   Optional[FanDuelProp] = None
    pin_under_equiv:  Optional[FanDuelProp] = None


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(
    r"\b(jr\.?|sr\.?|ii|iii|iv|v)\b", re.IGNORECASE
)
_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove punctuation/suffixes, collapse whitespace."""
    # Unicode normalization to strip accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = _SUFFIX_RE.sub("", name)
    name = _PUNCT_RE.sub("", name)
    name = _SPACE_RE.sub(" ", name).strip()
    return name


def normalize_prop_type(raw: str, league: str) -> Optional[str]:
    """
    Map a raw FanDuel prop type string to the canonical PrizePicks stat_type label,
    using league-aware dictionary lookup.
    Returns None if the prop type is unrecognized (should be skipped).
    """
    key = raw.lower().strip()
    return PROP_TYPE_MAP.get(league.upper(), {}).get(key)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def match_props(
    fd_props: list[FanDuelProp],
    dk_props: list[FanDuelProp],
    pp_lines: list[PrizePickLine],
    pin_props: list[FanDuelProp] | None = None,
) -> list[MatchedProp]:
    """
    For each PrizePicks line, find the best matching FanDuel, DraftKings,
    and Pinnacle prop.
    Only returns matches where at least one book provides a line for that prop.
    """
    pin_props = pin_props or []

    # Build lookup: (league, prop_type_lower) → list of props per book
    fd_index: dict[tuple, list[FanDuelProp]] = {}
    for fd in fd_props:
        key = (fd.league.upper(), fd.prop_type.lower())
        fd_index.setdefault(key, []).append(fd)

    dk_index: dict[tuple, list[FanDuelProp]] = {}
    for dk in dk_props:
        key = (dk.league.upper(), dk.prop_type.lower())
        dk_index.setdefault(key, []).append(dk)

    pin_index: dict[tuple, list[FanDuelProp]] = {}
    for pin in pin_props:
        key = (pin.league.upper(), pin.prop_type.lower())
        pin_index.setdefault(key, []).append(pin)

    # League-aware alias map: PrizePicks stat names that differ from book prop names
    # Only apply aliases for specific leagues to avoid cross-league pollution
    _STAT_ALIASES = {
        "SOCCER": {
            "saves": "goalie saves",
            "goalkeeper saves": "goalie saves",
        },
    }

    results: list[MatchedProp] = []

    for pp in pp_lines:
        stat_key = pp.stat_type.lower()
        league_aliases = _STAT_ALIASES.get(pp.league.upper(), {})
        stat_key = league_aliases.get(stat_key, stat_key)
        key = (pp.league.upper(), stat_key)

        fd_candidates = fd_index.get(key, [])
        dk_candidates = dk_index.get(key, [])
        pin_candidates = pin_index.get(key, [])

        # We need at least one book to compare against
        if not fd_candidates and not dk_candidates and not pin_candidates:
            continue

        norm_pp_name = normalize_name(pp.player_name)

        pp_is_whole = float(pp.line_score).is_integer()

        def _best_match(candidates):
            """
            Compare PP name against each book candidate using a composite
            score that handles:
              • exact token ordering         → token_sort_ratio
              • inserted/dropped middle names → token_set_ratio
                  ("Simeon Richardson" vs "Simeon Woods Richardson")
              • prefix insertions (Da/De/Van) → token_set_ratio
                  ("Tristan Silva" vs "Tristan Da Silva")

            We keep a conservative bar (FUZZY_THRESHOLD) applied to the
            *weakest* of (sort, set) — if either metric drops below the
            threshold, we treat it as uncertain and reject. This prevents
            common short-name collisions ("Miles McBride" vs "Mikal
            Bridges") while still letting middle-name insertions through.

            Returns the per-(player, prop) match as a tuple
            (exact, over_equiv, under_equiv, score). Books often post
            multiple alt lines for the same player; once the player is
            identified by name we look across that player's lines for
            exact-line and PP±0.5 half-step equivalents, so a PP whole
            number can be priced off the book's nearest valid alt line
            instead of being thrown out.
            """
            best = None
            best_score = 0.0
            best_norm_name: str | None = None
            for c in candidates:
                norm_cand = normalize_name(c.player_name)
                sort_score = fuzz.token_sort_ratio(norm_pp_name, norm_cand)
                set_score  = fuzz.token_set_ratio(norm_pp_name, norm_cand)

                # An exact subset match (set=100) where the sort score is
                # also reasonably high should be accepted. We require the
                # shorter name's tokens to all appear in the longer name,
                # with sort_score >= 80 to reject accidental overlaps.
                if set_score >= 100 and sort_score >= 80:
                    score = set_score
                else:
                    # Otherwise fall back to the stricter sort-based score.
                    score = sort_score

                if score > best_score:
                    best_score = score
                    best = c
                    best_norm_name = norm_cand
                elif score == best_score and score >= FUZZY_THRESHOLD and best is not None:
                    # Prefer exact-line first, then half-step equivalents on
                    # whole-number PP lines, over an arbitrary same-name pick.
                    cand_match = (c.line == pp.line_score)
                    cur_match = (best.line == pp.line_score)
                    if cand_match and not cur_match:
                        best = c
                        best_norm_name = norm_cand
                    elif pp_is_whole and not cur_match:
                        cand_half = c.line in (pp.line_score + 0.5, pp.line_score - 0.5)
                        cur_half = best.line in (pp.line_score + 0.5, pp.line_score - 0.5)
                        if cand_half and not cur_half:
                            best = c
                            best_norm_name = norm_cand
            if best is None or best_score < FUZZY_THRESHOLD:
                return None, None, None, 0.0

            # Walk the candidate list once more, this time keeping every
            # row that belongs to the matched player (same normalized name)
            # so we can locate the exact-line and half-step alts.
            same_player = [c for c in candidates if normalize_name(c.player_name) == best_norm_name]

            exact = next((c for c in same_player if c.line == pp.line_score), None)
            over_eq = exact
            under_eq = exact
            if exact is None and pp_is_whole:
                # Half-step equivalence: over PP-N == over book-(N+0.5)
                # under push-on-tie. Require the book to actually post the
                # relevant side at that line — a half-step entry that only
                # has under_odds isn't a valid over equivalent.
                plus_half  = next((c for c in same_player if c.line == pp.line_score + 0.5 and c.over_odds  is not None), None)
                minus_half = next((c for c in same_player if c.line == pp.line_score - 0.5 and c.under_odds is not None), None)
                over_eq  = plus_half
                under_eq = minus_half

            return exact, over_eq, under_eq, best_score

        fd_exact,  fd_over,  fd_under,  fd_score  = _best_match(fd_candidates)
        dk_exact,  dk_over,  dk_under,  dk_score  = _best_match(dk_candidates)
        pin_exact, pin_over, pin_under, pin_score = _best_match(pin_candidates)

        # Keep a match when at least one book has either an exact-line
        # entry or a usable half-step equivalent for some side.
        any_book = (
            fd_exact  or fd_over  or fd_under  or
            dk_exact  or dk_over  or dk_under  or
            pin_exact or pin_over or pin_under
        )
        if not any_book:
            continue

        scores = [s for s in (fd_score, dk_score, pin_score) if s >= FUZZY_THRESHOLD]
        if not scores:
            continue

        results.append(MatchedProp(
            pp=pp,
            fd=fd_exact,
            dk=dk_exact,
            pin=pin_exact,
            fd_over_equiv=fd_over,
            fd_under_equiv=fd_under,
            dk_over_equiv=dk_over,
            dk_under_equiv=dk_under,
            pin_over_equiv=pin_over,
            pin_under_equiv=pin_under,
            name_score=max(scores),
        ))

    return results
