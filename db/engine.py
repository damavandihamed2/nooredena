from __future__ import annotations

import os
from typing import Generator
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


load_dotenv()
conn_str = os.getenv('SQLSERVER_DSN', 'default_key')


engine = create_engine(
    conn_str,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    echo=False,          # اگر خواستی SQLهایی که اجرا می‌شن را ببینی True کن
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    class_=Session,
)


def get_session() -> Generator[Session, None, None]:
    """Dependency-style helper (مثلاً بعداً برای FastAPI)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
