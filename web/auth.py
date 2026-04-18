import os
import time
import logging
import jwt
from jwt import PyJWKClient
from fastapi import Depends, HTTPException, Header
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")

# Supabase now signs user JWTs with ES256 using an asymmetric key published at
# the project's JWKS endpoint. Legacy projects may still use HS256 with the
# shared secret — we try JWKS first and fall back to the shared secret so both
# configurations work.
_JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json" if SUPABASE_URL else None
_jwk_client: Optional[PyJWKClient] = None
if _JWKS_URL:
    try:
        _jwk_client = PyJWKClient(_JWKS_URL, cache_keys=True, lifespan=3600)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to initialize PyJWKClient for %s: %s", _JWKS_URL, exc)
        _jwk_client = None


def _decode(token: str) -> Optional[dict]:
    """Verify a Supabase access token. Tries JWKS (ES256/RS256) first, then
    falls back to HS256 with SUPABASE_JWT_SECRET. Returns the payload or None."""
    try:
        header = jwt.get_unverified_header(token)
    except Exception:
        return None

    alg = header.get("alg", "")

    # Asymmetric: verify against Supabase JWKS.
    if alg in ("ES256", "RS256", "ES384", "RS384") and _jwk_client is not None:
        try:
            signing_key = _jwk_client.get_signing_key_from_jwt(token).key
            return jwt.decode(
                token,
                signing_key,
                algorithms=[alg],
                audience="authenticated",
            )
        except Exception as exc:
            logger.debug("JWKS verify failed: %s", exc)
            return None

    # Symmetric legacy path.
    if alg == "HS256" and SUPABASE_JWT_SECRET:
        try:
            return jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                audience="authenticated",
            )
        except Exception as exc:
            logger.debug("HS256 verify failed: %s", exc)
            return None

    return None


async def get_current_user_optional(authorization: Optional[str] = Header(None)) -> Optional[dict]:
    """Extract and verify Supabase JWT if present. Returns None if missing or invalid."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    payload = _decode(token)
    if not payload:
        return None
    return {"id": payload["sub"], "email": payload.get("email"), "jwt": token}


async def get_current_user(user: Optional[dict] = Depends(get_current_user_optional)) -> dict:
    """Extract and verify Supabase JWT. Raises 401 if missing or invalid."""
    if not user:
        raise HTTPException(status_code=401, detail="Valid bearer token required")
    return user
