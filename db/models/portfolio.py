from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, BigInteger, Float, Integer, String
from .base import Base


class PortfolioHoldings(Base):
    __tablename__ = "holdings"
    __table_args__ = {"schema": "portfolio"}

    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    sub_symbol: Optional[str] = Column(String(150), nullable=False, primary_key=True)
    sector: Optional[str] = Column(String(150), nullable=True)
    sub_sector: Optional[str] = Column(String(150), nullable=True)
    share: Optional[float] = Column(Float, nullable=True)


class PortfolioPortfolio(Base):
    __tablename__ = "portfolio"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(50), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    sub_sector: Optional[str] = Column(String(255), nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)


class PortfolioPortfolioDailyReport(Base):
    __tablename__ = "portfolio_daily_report"
    __table_args__ = {"schema": "portfolio"}

    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    sub_sector: Optional[str] = Column(String(255), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    symbol_name_eng: Optional[str] = Column(String(50), nullable=True)
    first_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)
    trade_number: Optional[int] = Column(Integer, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    day_low: Optional[int] = Column(Integer, nullable=True)
    day_high: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    sector: Optional[str] = Column(String(255), nullable=True)
    day_max: Optional[int] = Column(Integer, nullable=True)
    day_min: Optional[int] = Column(Integer, nullable=True)
    yval: Optional[int] = Column(Integer, nullable=True)
    sell_count: Optional[int] = Column(Integer, nullable=True)
    sell_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_price: Optional[int] = Column(Integer, nullable=True)
    buy_price: Optional[int] = Column(Integer, nullable=True)
    buy_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_count: Optional[int] = Column(Integer, nullable=True)
    buy_number_natural: Optional[int] = Column(Integer, nullable=True)
    buy_volume_natural: Optional[int] = Column(BigInteger, nullable=True)
    sell_number_natural: Optional[int] = Column(Integer, nullable=True)
    sell_volume_natural: Optional[int] = Column(BigInteger, nullable=True)
    buy_number_legal: Optional[int] = Column(Integer, nullable=True)
    buy_volume_legal: Optional[int] = Column(BigInteger, nullable=True)
    sell_number_legal: Optional[int] = Column(Integer, nullable=True)
    sell_volume_legal: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class PortfolioPortfolioDailyReportDiff(Base):
    __tablename__ = "portfolio_daily_report_diff"
    __table_args__ = {"schema": "portfolio"}

    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    first_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)
    trade_number: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)
    portfolio_id_y: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol_y: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    amount_y: Optional[int] = Column(BigInteger, nullable=True)
    close_price_y: Optional[int] = Column(Integer, nullable=True)
    trade_number_y: Optional[int] = Column(Integer, nullable=True)
    date_y: Optional[str] = Column(String(50), nullable=True)
    time_y: Optional[str] = Column(String(50), nullable=True)
    value_diff: Optional[int] = Column(BigInteger, nullable=True)


class PortfolioPortfolioDividend(Base):
    __tablename__ = "portfolio_dividend"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(20), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    dividend: Optional[int] = Column(BigInteger, nullable=True)


class PortfolioPortfolioOptions(Base):
    __tablename__ = "portfolio_options"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(50), nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    contract_size: Optional[int] = Column(Integer, nullable=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    strike_price: Optional[int] = Column(Integer, nullable=True)


class PortfolioPortfolioOptionsHistory(Base):
    __tablename__ = "portfolio_options_history"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    contract_size: Optional[int] = Column(Integer, nullable=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    strike_price: Optional[int] = Column(Integer, nullable=True)


class PortfolioPortfolioOptionsTemp(Base):
    __tablename__ = "portfolio_options_temp"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(50), nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    contract_size: Optional[int] = Column(Integer, nullable=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    strike_price: Optional[int] = Column(Integer, nullable=True)


class PortfolioPortfolioTemp(Base):
    __tablename__ = "portfolio_temp"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(50), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    sub_sector: Optional[str] = Column(String(255), nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)


class PortfolioPortfolioValue(Base):
    __tablename__ = "portfolio_value"
    __table_args__ = {"schema": "portfolio"}

    date: Optional[str] = Column(String(20), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    portfolio_value: Optional[int] = Column(BigInteger, nullable=True)
