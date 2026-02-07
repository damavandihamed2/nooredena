from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, Integer, String
from .base import Base

class TradersarenaSymbols(Base):
    __tablename__ = "symbols"
    __table_args__ = {"schema": "tradersarena"}

    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    SS0: Optional[int] = Column(Integer, nullable=True)
    SS0_date: Optional[str] = Column(String(50), nullable=True)
    SR0: Optional[int] = Column(Integer, nullable=True)
    SR0_date: Optional[str] = Column(String(50), nullable=True)
