from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, BigInteger, Float, Integer, SmallInteger, String, Text
from .base import Base


class BrokersBrokers(Base):
    __tablename__ = "brokers"
    __table_args__ = {"schema": "brokers"}

    portfolio_name: Optional[str] = Column(String(50), nullable=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    broker: Optional[str] = Column(String(255), nullable=True)
    broker_name: Optional[str] = Column(String(255), nullable=True)
    broker_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    broker_type: Optional[int] = Column(Integer, nullable=True)
    broker_url: Optional[str] = Column(String(255), nullable=True)
    broker_user: Optional[str] = Column(String(255), nullable=True)
    broker_pass: Optional[str] = Column(String(255), nullable=True)
    active: Optional[int] = Column(SmallInteger, nullable=True)
    purchase_upper_bond: Optional[int] = Column(BigInteger, nullable=True)
    remain: Optional[int] = Column(BigInteger, nullable=True)
    portfolio_value: Optional[int] = Column(BigInteger, nullable=True)
    last_month_trades: Optional[int] = Column(BigInteger, nullable=True)
    credit: Optional[int] = Column(BigInteger, nullable=True)

class BrokersPortfolioRayan(Base):
    __tablename__ = "portfolio_rayan"
    __table_args__ = {"schema": "brokers"}

    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    price: Optional[int] = Column(Integer, nullable=True)
    quantity: Optional[int] = Column(BigInteger, nullable=True)
    bourseAccount: Optional[str] = Column(String(50), nullable=True)
    displayName: Optional[str] = Column(String(255), nullable=True)
    stockValue: Optional[int] = Column(BigInteger, nullable=True)
    farsiName: Optional[str] = Column(String(255), nullable=True)
    isRight: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    rate: Optional[float] = Column(Float, nullable=True)
    marginAccountValue: Optional[float] = Column(Float, nullable=True)
    insMaxLCode: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    broker_id: Optional[str] = Column(String(10), nullable=False, primary_key=True)

class BrokersTrades(Base):
    __tablename__ = "trades"
    __table_args__ = {"schema": "brokers"}

    date: Optional[str] = Column(String(20), nullable=False, primary_key=True)
    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    broker_id: Optional[str] = Column(String(20), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    board: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    type: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_paid_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    volume: Optional[int] = Column(Integer, nullable=True)
    price: Optional[int] = Column(Integer, nullable=True)
    value: Optional[int] = Column(BigInteger, nullable=True)

class BrokersTradesLast(Base):
    __tablename__ = "trades_last"
    __table_args__ = {"schema": "brokers"}

    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    date: Optional[str] = Column(String(20), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    board: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    type: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    is_paid_ros: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    volume: Optional[int] = Column(Integer, nullable=True)
    value: Optional[int] = Column(BigInteger, nullable=True)
    price: Optional[float] = Column(Float, nullable=True)
    total_cost: Optional[int] = Column(BigInteger, nullable=True)
    description: Optional[str] = Column(String(255), nullable=True)

class BrokersTradesRayan(Base):
    __tablename__ = "trades_rayan"
    __table_args__ = {"schema": "brokers"}

    portfolio_id: Optional[int] = Column(Integer, nullable=True)
    row: Optional[int] = Column("row_", Integer, nullable=False, primary_key=True)
    transactionDate: Optional[str] = Column(String(50), nullable=True)
    amount: Optional[int] = Column(BigInteger, nullable=True)
    branch: Optional[str] = Column(String(255), nullable=True)
    comments: Optional[str] = Column(Text, nullable=True)
    fcKey: Optional[str] = Column(Text, nullable=True)
    qty: Optional[int] = Column(Integer, nullable=True)
    price: Optional[int] = Column(Integer, nullable=True)
    bourseAccount: Optional[str] = Column(String(50), nullable=True)
    branchId: Optional[int] = Column(Integer, nullable=True)
    csTypeId: Optional[int] = Column(SmallInteger, nullable=True)
    csTypeName: Optional[str] = Column(String(50), nullable=True)
    insMaxLCode: Optional[str] = Column(String(50), nullable=True)
    rowOrder: Optional[str] = Column(Text, nullable=True)
    debtor: Optional[int] = Column(BigInteger, nullable=True)
    creditor: Optional[int] = Column(BigInteger, nullable=True)
    remaining: Optional[int] = Column(BigInteger, nullable=True)
    broker_id: Optional[str] = Column(String(155), nullable=True)

class BrokersTradesTadbirLedger(Base):
    __tablename__ = "trades_tadbir_ledger"
    __table_args__ = {"schema": "brokers"}

    portfolio_id: Optional[int] = Column(Integer, nullable=True)
    row: Optional[int] = Column("row_", Integer, nullable=False, primary_key=True)
    Debitor: Optional[int] = Column(BigInteger, nullable=True)
    Creditor: Optional[int] = Column(BigInteger, nullable=True)
    Description: Optional[str] = Column(Text, nullable=True)
    MarketInstrumentId: Optional[int] = Column(Integer, nullable=True)
    MarketInstrumentTitle: Optional[str] = Column(String(255), nullable=True)
    MarketInstrumentISIN: Optional[str] = Column(String(50), nullable=True)
    TransactionDate: Optional[str] = Column(String(50), nullable=True)
    Price: Optional[int] = Column(Integer, nullable=True)
    Volume: Optional[int] = Column(BigInteger, nullable=True)
    BrokerStationCode: Optional[str] = Column(String(50), nullable=True)
    TradeNumber: Optional[str] = Column(String(50), nullable=True)
    Remain: Optional[int] = Column(BigInteger, nullable=True)
    broker_id: Optional[str] = Column(String(50), nullable=True)

class BrokersTradesTadbir(Base):
    __tablename__ = "trades_tadbir"
    __table_args__ = {"schema": "brokers"}

    portfolio_id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    Symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    CdsSymbol: Optional[str] = Column(String(50), nullable=True)
    TotalCommision: Optional[int] = Column(BigInteger, nullable=True)
    Volume: Optional[int] = Column(BigInteger, nullable=True)
    NetPrice: Optional[int] = Column(BigInteger, nullable=True)
    Price: Optional[int] = Column(Integer, nullable=True)
    TradeDate: Optional[str] = Column(String(155), nullable=False, primary_key=True)
    TradeId: Optional[int] = Column(Integer, nullable=True)
    TradeNumber: Optional[str] = Column(String(50), nullable=True)
    TradeSideTitle: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    VolumeInPrice: Optional[int] = Column(BigInteger, nullable=True)
    broker_id: Optional[str] = Column(String(155), nullable=False, primary_key=True)
