"""Unauthenticated endpoints the marketing landing page reads.

The landing page used to hardcode its numbers ("2,847 bets scanned today",
"4,200+ sharps", "58.4% backtested hit rate", "refreshed every 30 seconds").
None of them came from anywhere — they were invented, and two were provably
wrong: the scheduler's real cadence is `_state["interval_min"]` (5 min by
default, user-configurable), not 30 seconds, and the league list advertised NFL,
which `config.ACTIVE_LEAGUES` has never contained.

`/api/public/coverage` exists so the page can state coverage facts it reads
from the running process. Anything the server can't answer, the page omits
rather than guesses — see `useCoverage()` in web/static/components.jsx, shared
by both landing.jsx and pricing.jsx.

Everything here is deliberately public and carries no user data: it is the
same class of information as `/api/status`, which is already unauthenticated.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, field_validator

import config as cfg
from web.state import _lock, _state

router = APIRouter(prefix="/api/public", tags=["public"])

logger = logging.getLogger(__name__)

# The books the pipeline actually scrapes for pricing, in the order the +EV
# board shows them. PrizePicks is listed separately because it is the source of
# the LINES being evaluated, not one of the books being devigged against.
# Novig is conditional on cfg.NOVIG_ENABLED.
_PRICING_BOOKS = ("FanDuel", "DraftKings", "Pinnacle")

# Novig is a peer-to-peer exchange, not a sportsbook, so a flat "4 books" count
# is imprecise once it's in the list. `price_sources` is the honest noun for the
# mixed set and is what the marketing copy counts.
_NOVIG = "Novig"


@router.get("/coverage")
def get_coverage():
    """Coverage facts for the marketing pages: which books, which leagues, how
    often the board refreshes, and the trial length. No user data, no auth."""
    books = list(_PRICING_BOOKS)
    if cfg.NOVIG_ENABLED:
        books.append(_NOVIG)

    # Single-field read, but still under the lock — `interval_min` is mutated by
    # POST /api/config while the pipeline may be rebinding neighbours.
    with _lock:
        refresh_minutes = _state["interval_min"]

    # Lazy import: web/app.py imports this module, so a top-level import would
    # cycle. Same pattern as web/routers/admin.py's `_memory_snapshot` import.
    #
    # The trial length is served rather than written into the copy because
    # BILLING_TRIAL_DAYS is env-configurable and the pricing page stated "7" in
    # six places. Setting BILLING_TRIAL_DAYS=14 would have left every one of
    # them advertising a shorter trial than Stripe actually grants.
    from web.app import BILLING_TRIAL_DAYS

    return {
        "prop_source":     "PrizePicks",
        "books":           books,
        # The noun the copy should use for len(books). "4 books" is wrong once
        # the exchange is in the list; "4 price sources" is right either way.
        "books_noun":      "price sources" if _NOVIG in books else "sportsbooks",
        "leagues":         [lg for lg, on in cfg.ACTIVE_LEAGUES.items() if on],
        "refresh_minutes": refresh_minutes,
        "trial_days":      BILLING_TRIAL_DAYS,
    }


# ---------------------------------------------------------------------------
# Landing minigame — daily pick, reveal, telemetry, headshot proxy
#
# Same doctrine as /coverage: anything the server can't answer, the page omits
# rather than guesses. Two additional rules specific to the game:
#
#   1. The pre-pick payload (GET /daily-pick) must not contain the answer in
#      any form — no probabilities, no favored side, no book odds. A visitor
#      with the Network tab open learns nothing before committing to a pick.
#   2. The reveal's numbers are computed from exactly the book quotes it
#      returns (web/minigame.py::devig_books), so the receipt is re-derivable
#      by hand. See web/minigame.py for the selection/freeze rules.
# ---------------------------------------------------------------------------

# ── Per-IP sliding-window rate limiter ─────────────────────────────────────
# In-memory on purpose: single worker, and the goal is only to stop a naive
# loop from hammering the unauthenticated endpoints — not to be a real WAF.
# Every public route enforces it, including the image proxy (a cache miss
# there does a network fetch on the shared sync threadpool).
_RATE_LIMIT = 60          # requests
_RATE_WINDOW_SEC = 60.0   # per sliding window
_rate_lock = threading.Lock()
_rate_hits: dict[str, deque] = {}


# Hard ceiling on tracked IPs. Stale-entry eviction alone is not a bound: a
# burst of distinct spoofed addresses inside one window is all "fresh", so
# freshness-based eviction removes nothing while the dict grows without limit.
_RATE_MAX_IPS = 4096


def _client_ip(request: Request) -> str:
    # Render terminates TLS at a proxy, so the peer address is the proxy's.
    # Use the RIGHTMOST X-Forwarded-For hop: the proxy APPENDS the real peer
    # to whatever the client sent, so the leftmost value is attacker-chosen
    # (keying on it let one caller mint a fresh rate bucket per request).
    # The rightmost is the only hop our own proxy vouches for.
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[-1].strip()
    return request.client.host if request.client else "unknown"


def _enforce_rate_limit(request: Request) -> None:
    now = time.monotonic()
    ip = _client_ip(request)
    with _rate_lock:
        hits = _rate_hits.get(ip)
        if hits is None:
            hits = _rate_hits[ip] = deque()
        while hits and now - hits[0] > _RATE_WINDOW_SEC:
            hits.popleft()
        if len(hits) >= _RATE_LIMIT:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
        hits.append(now)
        # Bound the table: drop idle IPs so a slow scan over many addresses
        # can't grow this dict forever...
        if len(_rate_hits) > _RATE_MAX_IPS:
            for stale_ip in [k for k, v in _rate_hits.items() if not v or now - v[-1] > _RATE_WINDOW_SEC]:
                _rate_hits.pop(stale_ip, None)
            # ...and if everything is fresh (spoofed-address burst), evict
            # oldest-first-seen regardless. Losing a live bucket only resets
            # one caller's window — strictly better than unbounded growth.
            while len(_rate_hits) > _RATE_MAX_IPS:
                _rate_hits.pop(next(iter(_rate_hits)), None)


# Serialized daily-pick bodies, keyed by the board they were built from. A
# blob is immutable once adopted and the only mutation is the once-a-day
# provisional -> earned upgrade, so the bytes and the tag are constant for the
# life of the board — yet the pre-memo code paid a 3x13-key dict rebuild, a
# json.dumps and an md5 on EVERY request, including the 304s render.yaml's
# `--threads 1` comment assumes are nearly free. HEADSHOTS_ENABLED is part of
# the key because it changes image_url without changing the board.
# Bounded: at most a handful of boards are live at once (today's, plus the
# superseded ones still being served to mid-game visitors).
_daily_payload_cache: dict[tuple, tuple[bytes, str]] = {}
_daily_payload_lock = threading.Lock()
_DAILY_PAYLOAD_MAX = 8

# Cap on ?have= — the client sends the ids it is mid-game on, which is at most
# one board's worth. Anything longer is not a real client.
_HAVE_MAX_IDS = 6


def _if_none_match_matches(header: str | None, etag: str) -> bool:
    """RFC 9110 If-None-Match: a comma-separated LIST of tags, or `*`. Exact
    single-value equality made every multi-tag client pay for a full 200."""
    if not header:
        return False
    for tag in header.split(","):
        tag = tag.strip()
        if tag == "*" or tag == etag:
            return True
    return False


def _daily_payload(idx: int, blob: dict) -> tuple[bytes, str]:
    key = (idx, blob.get("frozen_at"), blob.get("status"), cfg.HEADSHOTS_ENABLED)
    with _daily_payload_lock:
        hit = _daily_payload_cache.get(key)
    if hit is not None:
        return hit

    picks = []
    for p in blob.get("picks", []):
        has_image = bool(p.get("image_source_url")) and cfg.HEADSHOTS_ENABLED
        picks.append({
            "id":             p["id"],
            "player":         p.get("player"),
            "team":           p.get("team"),
            "opponent":       p.get("opponent"),
            "position":       p.get("position"),
            "league":         p.get("league"),
            "prop":           p.get("prop"),
            "line":           p.get("line"),
            "game_start":     p.get("game_start"),
            "trending_count": p.get("trending_count", 0),
            # The proxy URL, never the PrizePicks URL: the source stays
            # server-side so the page can't be told to hotlink PP, and the id
            # gate below keeps this from being an open proxy.
            "image_url":      f"/api/public/player-image?pick={p['id']}" if has_image else None,
        })
    payload = json.dumps({"day_index": idx, "picks": picks},
                         separators=(",", ":"), default=str).encode("utf-8")
    # Weak (W/) for the same reason as web/app.py::_build_etag: gzip at the
    # transport layer can mutate the bytes a strong tag would promise.
    entry = (payload, 'W/"%s"' % hashlib.md5(payload).hexdigest()[:16])
    with _daily_payload_lock:
        if len(_daily_payload_cache) >= _DAILY_PAYLOAD_MAX:
            _daily_payload_cache.clear()
        _daily_payload_cache[key] = entry
    return entry


@router.get("/daily-pick")
def get_daily_pick(request: Request, have: Optional[str] = None):
    """Today's frozen selection, answer-free. See module comment rule #1.

    `have` is the dot-separated list of pick ids the caller is already mid-game
    on. The day allows one board change (provisional -> earned); for a visitor
    who has already played, that change would drop their plays and hand them
    three fresh free picks, so a caller that proves it holds a board this
    process served keeps being served that board (web/minigame.py::board_for_ids).

    Served with the same weak-ETag/304 contract as the authenticated payloads
    in web/app.py::_cached_response. This is the one endpoint every landing-page
    hit calls, including bots, so a repeat visitor inside the same board should
    cost a 304 and not a re-serialized body — and the body it would have
    re-serialized is memoized per board, so a 304 costs a dict lookup.
    """
    _enforce_rate_limit(request)
    # Lazy import (same reason as BILLING_TRIAL_DAYS above): web.minigame
    # imports web.state only, but keeping router-level imports minimal keeps
    # the import graph obvious.
    from web import minigame

    idx, blob = minigame.get_or_freeze_today()
    if have:
        held = [t for t in have.replace(",", ".").split(".") if t][:_HAVE_MAX_IDS]
        blob = minigame.board_for_ids(idx, blob, held)

    payload, etag = _daily_payload(idx, blob)
    if _if_none_match_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers={"ETag": etag})
    # no-cache, not max-age: the board can upgrade once mid-day (provisional ->
    # earned), and a stale copy on an intermediary would show a visitor a board
    # whose ids the reveal endpoint may no longer be serving from today's blob.
    return Response(content=payload, media_type="application/json",
                    headers={"ETag": etag, "Cache-Control": "no-cache"})


@router.get("/daily-pick/reveal")
def get_daily_pick_reveal(request: Request, id: str):
    """The answer for one pick, after the visitor commits. p_more/p_less/
    vig_pct were computed from exactly the books array returned here
    (web/minigame.py::devig_books) — the receipt is re-derivable by hand."""
    _enforce_rate_limit(request)
    from web import minigame

    # find_pick searches today's blob, then the boards today has already
    # replaced, then the recent frozen days: a visitor's board can outlive the
    # blob that served it (tab open across the 8am ET boundary, or the
    # provisional board upgraded while they were reading it). Grading from the
    # pick's own board is exact — the alternative is 404ing every card the
    # visitor holds into a permanent "tap to try again" loop. It is a pure
    # read: grading must never be what moves the board.
    p = minigame.find_pick(id)
    if p is None:
        raise HTTPException(status_code=404, detail="Unknown pick id")
    return {
        "id":      p["id"],
        "favored": p["favored"],
        "p_more":  p["p_more"],
        "p_less":  p["p_less"],
        "vig_pct": p["vig_pct"],
        "books":   p["books"],
    }


# meta is a jsonb column with no DB-side cap; an unauthenticated endpoint must
# bound what it persists. The frontend's biggest meta is {"side": "less"} — a
# few tens of bytes — so 2KB is generous headroom, not a constraint.
_EVENT_META_MAX_BYTES = 2048

# Telemetry writes drain through one small pool instead of a thread per
# request: per-request threads under flood meant unbounded OS threads + DB
# connections. The semaphore is the queue bound — when it's exhausted the
# event is shed (204 regardless; telemetry must never matter that much).
_event_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="landing_event")
_event_slots = threading.Semaphore(64)


class LandingEvent(BaseModel):
    """Funnel telemetry. The enum is closed on purpose — a free-text event
    field on an unauthenticated endpoint becomes a junk drawer immediately."""
    event: Literal["game_viewed", "pick_made", "revealed", "plays_exhausted", "cta_clicked"]
    day_index: int
    pick_id: Optional[str] = None
    meta: Optional[dict] = None

    @field_validator("meta")
    @classmethod
    def _meta_bounded(cls, v: Optional[dict]) -> Optional[dict]:
        if v is not None:
            serialized = json.dumps(v, separators=(",", ":"), default=str)
            if len(serialized) > _EVENT_META_MAX_BYTES:
                raise ValueError(f"meta exceeds {_EVENT_META_MAX_BYTES} bytes")
        return v


@router.post("/event", status_code=204)
def post_landing_event(request: Request, body: LandingEvent):
    """Fire-and-forget funnel event: the visitor's response never waits on
    Supabase, and a write failure is logged, not surfaced."""
    _enforce_rate_limit(request)

    row = {
        "ts":        datetime.now(timezone.utc).isoformat(),
        "event":     body.event,
        "day_index": body.day_index,
        "pick_id":   body.pick_id,
        "meta":      body.meta or {},
    }

    def _log_landing_event_bg(row=row):
        try:
            # writer() per CLAUDE.md: new write paths go through the audited
            # service-role seam. When Supabase env is absent the shim's
            # execute() raises and we drop the event silently.
            from engine.writer import writer
            writer("landing.event").table("landing_events").insert(row).execute()
        except Exception as exc:
            logger.debug("landing_events insert failed: %s", exc)
        finally:
            _event_slots.release()

    if _event_slots.acquire(blocking=False):
        try:
            _event_pool.submit(_log_landing_event_bg)
        except RuntimeError:  # interpreter shutdown
            _event_slots.release()
    # else: queue full — shed the event rather than grow anything unbounded.
    return Response(status_code=204)


# ── Headshot proxy ─────────────────────────────────────────────────────────
# pick_id -> (content_type, bytes). Only today's (at most 3) picks are ever
# cached, so this stays tiny; cleared wholesale when it grows past a bound
# rather than tracking day rollover explicitly.
_image_cache: dict[str, tuple[str, bytes]] = {}
_image_cache_lock = threading.Lock()
# Single-flight for upstream fetches: N concurrent misses for the same pick
# must not fire N parallel 5s httpx calls out of the shared sync threadpool
# (thundering herd). One coarse lock is enough at <= 3 images per day —
# waiters re-check the byte cache after acquiring.
_image_fetch_lock = threading.Lock()
_IMAGE_CACHE_MAX = 32
_IMAGE_FETCH_TIMEOUT_SEC = 5.0
# Hard byte cap on the streamed upstream body. Real PP headshots are tens of
# KB; anything past 2MB is not a headshot and must not be buffered whole into
# the 512MB worker.
_IMAGE_MAX_BYTES = 2 * 1024 * 1024
# Browser-ish headers: PP's CDN serves these publicly to the app, and the
# scraper already presents the same Referer (scrapers/prizepicks.py).
_IMAGE_FETCH_HEADERS = {
    "Referer": "https://app.prizepicks.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "image/avif,image/webp,image/png,image/*,*/*;q=0.8",
}


def _fetch_image(source_url: str) -> tuple[str, bytes]:
    """Streamed, capped, image-only fetch of a server-chosen URL.

    Raises on anything suspicious; the caller maps every failure to 404.
    Hardenings, each load-bearing:
      * https only — the id gate controls the first URL, this controls its
        scheme.
      * no redirect following — a 30x could point the fetch at a host the id
        gate never approved (link-local/metadata addresses included).
      * content-type must be image/* — the bytes are re-served from OUR
        origin, so upstream text/html would become first-party active
        content (and get cached for the whole day).
      * streamed with a byte cap — resp.content would buffer an arbitrarily
        large body into the 512MB worker.
    """
    if not source_url.lower().startswith("https://"):
        raise ValueError("non-https source url")
    import httpx
    with httpx.stream(
        "GET",
        source_url,
        headers=_IMAGE_FETCH_HEADERS,
        timeout=_IMAGE_FETCH_TIMEOUT_SEC,
        follow_redirects=False,
    ) as resp:
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
        if not content_type.startswith("image/"):
            raise ValueError(f"upstream content-type {content_type!r} is not an image")
        chunks: list[bytes] = []
        size = 0
        for chunk in resp.iter_bytes():
            size += len(chunk)
            if size > _IMAGE_MAX_BYTES:
                raise ValueError("upstream body exceeds byte cap")
            chunks.append(chunk)
        return content_type, b"".join(chunks)


@router.get("/player-image")
def get_player_image(request: Request, pick: str):
    """Proxied player headshot for one of the frozen picks.

    The id gate is the whole security story: only ids present in a recent
    frozen selection resolve (find_pick — same board-swap tolerance as the
    reveal endpoint), so this cannot be used as an open proxy — the server
    chooses the upstream URL, never the caller. Rate-limited like the other
    public endpoints: cache hits are cheap, but a miss does a network fetch
    on the shared sync threadpool and an unauthenticated burst of misses
    must not be free.
    """
    _enforce_rate_limit(request)

    # Kill switch (config.HEADSHOTS_ENABLED). 404 rather than 403/503 so the
    # frontend's single "no image -> fallback avatar" path covers it.
    if not cfg.HEADSHOTS_ENABLED:
        raise HTTPException(status_code=404, detail="Not found")

    from web import minigame

    p = minigame.find_pick(pick)
    source_url = p.get("image_source_url") if p else None
    if not source_url:
        raise HTTPException(status_code=404, detail="Not found")

    with _image_cache_lock:
        cached = _image_cache.get(pick)
    if cached is None:
        # Single-flight: concurrent misses queue here; whoever fetched first
        # populates the cache and the rest find it on re-check.
        with _image_fetch_lock:
            with _image_cache_lock:
                cached = _image_cache.get(pick)
            if cached is None:
                try:
                    cached = _fetch_image(source_url)
                except Exception as exc:
                    # 404, not 5xx: the frontend treats any image failure as
                    # "use the fallback avatar", and a transient CDN error
                    # shouldn't look like a server bug in our own monitoring.
                    logger.debug("player-image fetch failed for %s: %s", pick, exc)
                    raise HTTPException(status_code=404, detail="Not found")
                with _image_cache_lock:
                    if len(_image_cache) >= _IMAGE_CACHE_MAX:
                        _image_cache.clear()
                    _image_cache[pick] = cached

    content_type, body = cached
    return Response(
        content=body,
        media_type=content_type,
        headers={
            # Long client cache: the image for a given pick id never changes
            # within the day it's valid, and the id itself rotates daily.
            "Cache-Control": "public, max-age=86400, immutable",
            # The media_type is validated image/*, but never let a browser
            # second-guess bytes served from our origin.
            "X-Content-Type-Options": "nosniff",
        },
    )
