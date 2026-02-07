from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, BigInteger, Float, Integer, String, Text
from .base import Base


class RahavardCapitalChanges(Base):
    __tablename__ = "capital_changes"
    __table_args__ = {"schema": "rahavard"}

    id: Optional[str] = Column(String(15), nullable=False, primary_key=True)
    company: Optional[str] = Column(String(15), nullable=True)
    asset_id: Optional[str] = Column(String(15), nullable=True)
    report: Optional[str] = Column(String(15), nullable=True)
    meeting: Optional[str] = Column(String(15), nullable=True)
    underwriting_end_report: Optional[str] = Column(String(15), nullable=True)
    registration_report: Optional[str] = Column(String(15), nullable=True)
    stock_certificate_receive_report: Optional[str] = Column(String(15), nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    underwriting_end_date: Optional[str] = Column(String(50), nullable=True)
    registration_date: Optional[str] = Column(String(50), nullable=True)
    stock_certificate_receive_date: Optional[str] = Column(String(50), nullable=True)
    warrant_sell_date: Optional[str] = Column(String(50), nullable=True)
    comments: Optional[str] = Column(Text, nullable=True)
    english_comments: Optional[str] = Column(String(50), nullable=True)
    previous_capital: Optional[int] = Column(BigInteger, nullable=True)
    new_capital: Optional[int] = Column(BigInteger, nullable=True)
    contribution: Optional[int] = Column(BigInteger, nullable=True)
    premium: Optional[int] = Column(BigInteger, nullable=True)
    reserve: Optional[int] = Column(BigInteger, nullable=True)


class RahavardDps(Base):
    __tablename__ = "dps"
    __table_args__ = {"schema": "rahavard"}

    pure_payout_ratio: Optional[float] = Column(Float, nullable=True)
    fiscal_year: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date_time: Optional[str] = Column(String(50), nullable=True)
    pure_eps: Optional[int] = Column(Integer, nullable=True)
    pe: Optional[int] = Column(Integer, nullable=True)
    report_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    pure_dps: Optional[int] = Column(Integer, nullable=True)
    capital: Optional[int] = Column(BigInteger, nullable=True)
    announcement_date: Optional[str] = Column(String(50), nullable=True)
    company_id: Optional[str] = Column(String(50), nullable=True)
    price_before_meeting: Optional[int] = Column(Integer, nullable=True)
    asset_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class RahavardDpsFunds(Base):
    __tablename__ = "dps_funds"
    __table_args__ = {"schema": "rahavard"}

    id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    fund_id: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    dividend: Optional[int] = Column(Integer, nullable=True)
    asset_id: Optional[str] = Column(String(50), nullable=True)


class RahavardSymbols(Base):
    __tablename__ = "symbols"
    __table_args__ = {"schema": "rahavard"}

    symbol: Optional[str] = Column(String(50), nullable=True)
    asset_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    slug: Optional[str] = Column(String(50), nullable=True)
    state: Optional[str] = Column(String(50), nullable=True)
    last_date: Optional[str] = Column(String(50), nullable=True)
    description: Optional[str] = Column(String(255), nullable=True)
    type: Optional[str] = Column(String(50), nullable=True)


class RahavardSymbolsDetail(Base):
    __tablename__ = "symbols_detail"
    __table_args__ = {"schema": "rahavard"}

    symbol: Optional[str] = Column(String(50), nullable=True)
    asset_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    slug: Optional[str] = Column(String(50), nullable=True)
    state: Optional[str] = Column(String(50), nullable=True)
    last_date: Optional[str] = Column(String(50), nullable=True)
    description: Optional[str] = Column(String(255), nullable=True)
    pure_payout_ratio: Optional[float] = Column(Float, nullable=True)
    pe_ttm: Optional[float] = Column(Float, nullable=True)
    last_free_float: Optional[float] = Column(Float, nullable=True)
    fiscal_month: Optional[int] = Column(Integer, nullable=True)
