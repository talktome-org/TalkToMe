import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client

def _backend_root() -> Path:
    return Path(__file__).resolve().parents[1]


load_dotenv(dotenv_path=_backend_root() / ".env")


def _init_supabase_client() -> Client:
    url: Optional[str] = os.getenv("SUPABASE_URL")
    key: Optional[str] = os.getenv("SUPABASE_SECRET_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SECRET_KEY in environment")
    return create_client(url, key)


supabase: Client = _init_supabase_client()


