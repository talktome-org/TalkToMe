import os
from typing import Optional

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jwt import PyJWKClient

security = HTTPBearer()

class SupabaseAuth:
    def __init__(self):
        base_url: Optional[str] = os.getenv("SUPABASE_URL")
        if not base_url:
            raise ValueError("Missing SUPABASE_URL in environment")

        base_url = base_url.rstrip("/")
        self.issuer: str = f"{base_url}/auth/v1"
        self.jwks_url: str = os.getenv("SUPABASE_JWKS_URL", f"{self.issuer}/.well-known/jwks.json")
        self.leeway_seconds = 60
        self.algorithms = ["RS256", "ES256"]

        try:
            self.jwk_client = PyJWKClient(self.jwks_url)
        except Exception as e:
            raise RuntimeError("Failed to initialize JWKS client") from e

    def _get_signing_key(self, token: str):
        try:
            return self.jwk_client.get_signing_key_from_jwt(token).key
        except Exception as e:
            raise HTTPException(status_code = 401, detail = f"Unable to fetch signing key: {str(e)}")

    def verify_jwt(self, token: str) -> dict:
        try:
            public_key = self._get_signing_key(token)
            payload = jwt.decode(
                token,
                public_key,
                algorithms=self.algorithms,
                audience = "authenticated",
                issuer = self.issuer,
                options = {"require": ["exp", "iat", "iss", "sub"]},
                leeway = self.leeway_seconds,
            )
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code = 401, detail = "Invalid or expired token")
        except (jwt.InvalidAudienceError, jwt.InvalidIssuerError, jwt.InvalidTokenError):
            raise HTTPException(status_code = 401, detail = "Invalid or expired token")

    def get_current_user(self, credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
        token = credentials.credentials
        return self.verify_jwt(token)

_auth_instance: Optional[SupabaseAuth] = None


def _get_auth() -> SupabaseAuth:
    """
    Lazily initialize auth so missing env vars don't crash the app at import time.
    Endpoints that depend on auth will return a clear error until configuration is present.
    """
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = SupabaseAuth()
    return _auth_instance


# Dependency for protected routes
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    try:
        auth = _get_auth()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Auth not configured: {str(e)}")

    try:
        return auth.verify_jwt(credentials.credentials)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid or expired token: {str(e)}")

