from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, String
from .base import Base

class TgjuTgjuSymbols(Base):
    __tablename__ = "tgju_symbols"
    __table_args__ = {"schema": "tgju"}

    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_en: Optional[str] = Column(String(150), nullable=True)
    symbol_fa: Optional[str] = Column(String(150), nullable=True)
