import os
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"connect_timeout": 5})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Supabase auth schema table stub.
# We reference auth.users via ForeignKey("auth.users.id") in several ORM models.
# SQLAlchemy ORM flush may sort tables by FK dependencies and will raise
# NoReferencedTableError if the referenced table isn't present in metadata.
Table(
    "users",
    Base.metadata,
    Column("id", PG_UUID(as_uuid=True), primary_key=True),
    schema="auth",
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()