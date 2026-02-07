from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, BigInteger, Integer, SmallInteger, String
from .base import Base


class CompanyCashflow(Base):
    __tablename__ = "cashflow"
    __table_args__ = {"schema": "company"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    meeting_date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    source: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    description: Optional[str] = Column(String(500), nullable=True)
    debtor: Optional[int] = Column(BigInteger, nullable=True)
    creditor: Optional[int] = Column(BigInteger, nullable=True)


class CompanyDividend(Base):
    __tablename__ = "dividend"
    __table_args__ = {"schema": "company"}

    portfolio_id: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    eps: Optional[int] = Column(Integer, nullable=True)
    dps: Optional[int] = Column(Integer, nullable=True)
    volume: Optional[int] = Column(BigInteger, nullable=True)
    value: Optional[int] = Column(BigInteger, nullable=True)


class CompanyPortfolioHistory(Base):
    __tablename__ = "portfolio_history"
    __table_args__ = {"schema": "company"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    is_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_paid_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    volume: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    final_price: Optional[int] = Column(Integer, nullable=True)


class CompanyPortfolioTransfers(Base):
    __tablename__ = "portfolio_transfers"
    __table_args__ = {"schema": "company"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    is_ros: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    is_paid_ros: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    volume: Optional[int] = Column(BigInteger, nullable=True)
    value: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    portfolio_id_new: Optional[int] = Column(Integer, nullable=False, primary_key=True)


class CompanyTrades(Base):
    __tablename__ = "trades"
    __table_args__ = {"schema": "company"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    type: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    is_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_paid_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    volume: Optional[int] = Column(BigInteger, nullable=True)
    value: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
