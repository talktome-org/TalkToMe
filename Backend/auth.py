import os
from pathlib import Path
import jwt
from jwt import PyJWKClient
from typing import Optional
from dotenv import load_dotenv
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

def _backend_root() -> Path:
    return Path(__file__).resolve().parent


load_dotenv(dotenv_path=_backend_root() / ".env")

security = HTTPBearer()

class SupabaseAuth:
    def __init__(self):
        base_url: Optional[str] = os.getenv("SUPABASE_URL")
        if not base_url:
            raise ValueError("Missing SUPABASE_URL in environment")

        base_url = base_url.rstrip("/")
        self.issuer: str = f"{base_url}/auth/v1"
        self.jwks_url: str = os.getenv("SUPABASE_JWKS_URL", f"{self.issuer}/keys")  # Allow override via SUPABASE_JWKS_URL, else derive from issuer
        self.leeway_seconds = int(os.getenv("JWT_LEEWAY_SECONDS", "60"))  # Small clock skew leeway (seconds)
        self.algorithms = [a.strip() for a in os.getenv("JWT_ALGORITHMS", "RS256,ES256").split(",") if a.strip()]

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

# Create auth instance
auth = SupabaseAuth()

# Dependency for protected routes
def get_current_user(user: dict = Depends(auth.get_current_user)) -> dict:
    return user

