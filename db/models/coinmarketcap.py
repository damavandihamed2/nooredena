from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, BigInteger, DateTime, Float, Integer, SmallInteger, String
from .base import Base


class CoinmarketcapCoinsHistory(Base):
    __tablename__ = "coins_history"
    __table_args__ = {"schema": "coinmarketcap"}

    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    cmc_rank: Optional[int] = Column(Integer, nullable=True)
    name: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    slug: Optional[str] = Column(String(50), nullable=True)
    num_market_pairs: Optional[int] = Column(Integer, nullable=True)
    date_added: Optional[datetime] = Column(DateTime, nullable=True)
    max_supply: Optional[int] = Column(BigInteger, nullable=True)
    circulating_supply: Optional[int] = Column(BigInteger, nullable=True)
    total_supply: Optional[int] = Column(BigInteger, nullable=True)
    infinite_supply: Optional[int] = Column(SmallInteger, nullable=True)
    tvl_ratio: Optional[float] = Column(Float, nullable=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)

class CoinmarketcapCoinsLatest(Base):
    __tablename__ = "coins_latest"
    __table_args__ = {"schema": "coinmarketcap"}

    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    cmc_rank: Optional[int] = Column(Integer, nullable=True)
    name: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    slug: Optional[str] = Column(String(50), nullable=True)
    num_market_pairs: Optional[int] = Column(Integer, nullable=True)
    date_added: Optional[datetime] = Column(DateTime, nullable=True)
    max_supply: Optional[int] = Column(BigInteger, nullable=True)
    circulating_supply: Optional[int] = Column(BigInteger, nullable=True)
    total_supply: Optional[int] = Column(BigInteger, nullable=True)
    infinite_supply: Optional[int] = Column(SmallInteger, nullable=True)
    tvl_ratio: Optional[float] = Column(Float, nullable=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)

class CoinmarketcapFearAndGreedHistory(Base):
    __tablename__ = "fear_and_greed_history"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[int] = Column(SmallInteger, nullable=True)
    update_time: Optional[datetime] = Column(DateTime, nullable=True)

class CoinmarketcapFearAndGreedLatest(Base):
    __tablename__ = "fear_and_greed_latest"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[int] = Column(SmallInteger, nullable=True)
    update_time: Optional[datetime] = Column(DateTime, nullable=True)

class CoinmarketcapGlobalMetricsHistory(Base):
    __tablename__ = "global_metrics_history"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)
    btc_dominance: Optional[float] = Column(Float, nullable=True)
    btc_dominance_yesterday: Optional[float] = Column(Float, nullable=True)
    btc_dominance_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    eth_dominance: Optional[float] = Column(Float, nullable=True)
    eth_dominance_yesterday: Optional[float] = Column(Float, nullable=True)
    eth_dominance_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    total_market_cap: Optional[float] = Column(Float, nullable=True)
    total_market_cap_yesterday: Optional[float] = Column(Float, nullable=True)
    total_market_cap_yesterday_percentage_change: Optional[float] = Column(Float, nullable=True)
    total_volume_24h: Optional[float] = Column(Float, nullable=True)
    total_volume_24h_yesterday: Optional[float] = Column(Float, nullable=True)
    total_volume_24h_yesterday_percentage_change: Optional[float] = Column(Float, nullable=True)
    altcoin_market_cap: Optional[float] = Column(Float, nullable=True)
    altcoin_volume_24h: Optional[float] = Column(Float, nullable=True)
    defi_market_cap: Optional[float] = Column(Float, nullable=True)
    defi_volume_24h: Optional[float] = Column(Float, nullable=True)
    defi_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    stablecoin_market_cap: Optional[float] = Column(Float, nullable=True)
    stablecoin_volume_24h: Optional[float] = Column(Float, nullable=True)
    stablecoin_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    derivatives_volume_24h: Optional[float] = Column(Float, nullable=True)
    derivatives_24h_percentage_change: Optional[float] = Column(Float, nullable=True)

class CoinmarketcapGlobalMetricsLatest(Base):
    __tablename__ = "global_metrics_latest"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)
    btc_dominance: Optional[float] = Column(Float, nullable=True)
    btc_dominance_yesterday: Optional[float] = Column(Float, nullable=True)
    btc_dominance_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    eth_dominance: Optional[float] = Column(Float, nullable=True)
    eth_dominance_yesterday: Optional[float] = Column(Float, nullable=True)
    eth_dominance_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    total_market_cap: Optional[float] = Column(Float, nullable=True)
    total_market_cap_yesterday: Optional[float] = Column(Float, nullable=True)
    total_market_cap_yesterday_percentage_change: Optional[float] = Column(Float, nullable=True)
    total_volume_24h: Optional[float] = Column(Float, nullable=True)
    total_volume_24h_yesterday: Optional[float] = Column(Float, nullable=True)
    total_volume_24h_yesterday_percentage_change: Optional[float] = Column(Float, nullable=True)
    altcoin_market_cap: Optional[float] = Column(Float, nullable=True)
    altcoin_volume_24h: Optional[float] = Column(Float, nullable=True)
    defi_market_cap: Optional[float] = Column(Float, nullable=True)
    defi_volume_24h: Optional[float] = Column(Float, nullable=True)
    defi_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    stablecoin_market_cap: Optional[float] = Column(Float, nullable=True)
    stablecoin_volume_24h: Optional[float] = Column(Float, nullable=True)
    stablecoin_24h_percentage_change: Optional[float] = Column(Float, nullable=True)
    derivatives_volume_24h: Optional[float] = Column(Float, nullable=True)
    derivatives_24h_percentage_change: Optional[float] = Column(Float, nullable=True)

class CoinmarketcapQuotesHistory(Base):
    __tablename__ = "quotes_history"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    price: Optional[float] = Column(Float, nullable=True)
    volume_24h: Optional[float] = Column(Float, nullable=True)
    volume_change_24h: Optional[float] = Column(Float, nullable=True)
    percent_change_1h: Optional[float] = Column(Float, nullable=True)
    percent_change_24h: Optional[float] = Column(Float, nullable=True)
    percent_change_7d: Optional[float] = Column(Float, nullable=True)
    percent_change_30d: Optional[float] = Column(Float, nullable=True)
    percent_change_60d: Optional[float] = Column(Float, nullable=True)
    percent_change_90d: Optional[float] = Column(Float, nullable=True)
    market_cap: Optional[float] = Column(Float, nullable=True)
    market_cap_dominance: Optional[float] = Column(Float, nullable=True)
    fully_diluted_market_cap: Optional[float] = Column(Float, nullable=True)
    tvl: Optional[float] = Column(Float, nullable=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)

class CoinmarketcapQuotesLatest(Base):
    __tablename__ = "quotes_latest"
    __table_args__ = {"schema": "coinmarketcap"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    price: Optional[float] = Column(Float, nullable=True)
    volume_24h: Optional[float] = Column(Float, nullable=True)
    volume_change_24h: Optional[float] = Column(Float, nullable=True)
    percent_change_1h: Optional[float] = Column(Float, nullable=True)
    percent_change_24h: Optional[float] = Column(Float, nullable=True)
    percent_change_7d: Optional[float] = Column(Float, nullable=True)
    percent_change_30d: Optional[float] = Column(Float, nullable=True)
    percent_change_60d: Optional[float] = Column(Float, nullable=True)
    percent_change_90d: Optional[float] = Column(Float, nullable=True)
    market_cap: Optional[float] = Column(Float, nullable=True)
    market_cap_dominance: Optional[float] = Column(Float, nullable=True)
    fully_diluted_market_cap: Optional[float] = Column(Float, nullable=True)
    tvl: Optional[float] = Column(Float, nullable=True)
    last_updated: Optional[datetime] = Column(DateTime, nullable=True)
