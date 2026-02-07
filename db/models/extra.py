from __future__ import annotations
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, BigInteger, Date, Float, Integer, LargeBinary, SmallInteger, String, Text
from .base import Base


class ExtraAuthTokens(Base):
    __tablename__ = "auth_tokens"
    __table_args__ = {"schema": "extra"}

    app: Optional[str] = Column(Text, nullable=False, primary_key=True)
    web_address: Optional[str] = Column(Text, nullable=False, primary_key=True)
    json_data: Optional[str] = Column(Text, nullable=False)


class ExtraAvalhamiNav(Base):
    __tablename__ = "avalhami_nav"
    __table_args__ = {"schema": "extra"}

    JalaliDate: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    Time: Optional[str] = Column(String(50), nullable=True)
    Units: Optional[int] = Column(BigInteger, nullable=True)
    PurchaseNAVPerShare: Optional[int] = Column(Integer, nullable=True)
    IssuedUnits: Optional[int] = Column(BigInteger, nullable=True)
    SellNAVPerShare: Optional[int] = Column(Integer, nullable=True)
    RevocedUnits: Optional[int] = Column(BigInteger, nullable=True)
    StatisticalNAVPerShare: Optional[int] = Column(Integer, nullable=True)
    NAV: Optional[int] = Column(BigInteger, nullable=True)
    Shareholder: Optional[int] = Column(Integer, nullable=True)


class ExtraAvalhamiTrades(Base):
    __tablename__ = "avalhami_trades"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[int] = Column(BigInteger, nullable=True)
    funds_unit: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    preferred_unit: Optional[int] = Column(SmallInteger, nullable=True)
    type: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    cost: Optional[int] = Column(BigInteger, nullable=True)


class ExtraCaptchaImages(Base):
    __tablename__ = "captcha_images"
    __table_args__ = {"schema": "extra"}

    captcha_type: Optional[str] = Column(String(255), nullable=True)
    captcha_image: bytes = Column(LargeBinary, nullable=False)
    captcha_value: Optional[str] = Column(String(50), nullable=True)
    captcha_id: Optional[str] = Column(String(255), nullable=False, primary_key=True)


class ExtraCbiRepoRate(Base):
    __tablename__ = "cbi_repo_rate"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    repo_rate: Optional[float] = Column(Float, nullable=True)
    min_repo_rate: Optional[float] = Column(Float, nullable=True)
    max_standing_facilities: Optional[float] = Column(Float, nullable=True)
    min_standing_facilities: Optional[float] = Column(Float, nullable=True)


class ExtraClosedDays(Base):
    __tablename__ = "closed_days"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(125), nullable=False, primary_key=True)


class ExtraDataInput(Base):
    __tablename__ = "data_input"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    user_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    category: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    param: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    type: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    fiscal_year: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    period: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[float] = Column(Float, nullable=True)


class ExtraDimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "extra"}

    Miladi: Optional[datetime] = Column(Date, nullable=True)
    Jalali_1: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    Jalali_2: Optional[str] = Column(String(255), nullable=True)
    Miladi_1: Optional[datetime] = Column("Miladi (#1)", Date, nullable=True)
    jyear: Optional[str] = Column(String(255), nullable=True)
    mmonthN: Optional[int] = Column(Integer, nullable=True)
    jmonthN: Optional[int] = Column(Integer, nullable=True)
    mmonthT: Optional[str] = Column(String(255), nullable=True)
    jmonthT: Optional[str] = Column(String(255), nullable=True)
    mnime: Optional[str] = Column(String(255), nullable=True)
    jnime: Optional[str] = Column(String(255), nullable=True)
    JquarterN: Optional[int] = Column(Integer, nullable=True)
    JQuarterT: Optional[str] = Column(String(255), nullable=True)
    MquarterN: Optional[int] = Column(Integer, nullable=True)
    JWeekDay: Optional[str] = Column(String(255), nullable=True)
    MWeekDay: Optional[str] = Column(String(255), nullable=True)
    MWeekNum: Optional[int] = Column(Integer, nullable=True)
    JWeekNum: Optional[int] = Column(Integer, nullable=True)
    index: Optional[int] = Column(Integer, nullable=True)


class ExtraExecRuntime(Base):
    __tablename__ = "exec_runtime"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    filename: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    action: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    duration: Optional[str] = Column(String(50), nullable=True)


class ExtraExpertsEstimates(Base):
    __tablename__ = "experts_estimates"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    user_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    category: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    param: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    type: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    fiscal_year: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    period: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[float] = Column(Float, nullable=True)


class ExtraExpertsSymbols(Base):
    __tablename__ = "experts_symbols"
    __table_args__ = {"schema": "extra"}

    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    user_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class ExtraLogs(Base):
    __tablename__ = "logs"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    directory: Optional[str] = Column(String(255), nullable=True)
    file_name: Optional[str] = Column(String(255), nullable=True)
    error_level: Optional[str] = Column(String(50), nullable=True)
    log_message: Optional[str] = Column(Text, nullable=True)
    line: Optional[int] = Column(Integer, nullable=True)


class ExtraMsgLast(Base):
    __tablename__ = "msg_last"
    __table_args__ = {"schema": "extra"}

    flow: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    last_time: Optional[str] = Column(String(50), nullable=True)
    last_date: Optional[str] = Column(String(50), nullable=True)


class ExtraNameMapper(Base):
    __tablename__ = "name_mapper"
    __table_args__ = {"schema": "extra"}

    name: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_id: Optional[str] = Column(String(155), nullable=False, primary_key=True)


class ExtraTradingInteligence(Base):
    __tablename__ = "trading_inteligence"
    __table_args__ = {"schema": "extra"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    portfolio_return_active: Optional[float] = Column(Float, nullable=True)
    portfolio_return_passive: Optional[float] = Column(Float, nullable=True)
    total_index_return: Optional[float] = Column(Float, nullable=True)
    price_index_eq_return: Optional[float] = Column(Float, nullable=True)
    investment_sector_index_return: Optional[float] = Column(Float, nullable=True)
    top_30_index_return: Optional[float] = Column(Float, nullable=True)
    inactivity_return: Optional[float] = Column(Float, nullable=True)
    start_date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value_diff_inactivity: Optional[int] = Column(BigInteger, nullable=True)
    value_diff_passive: Optional[int] = Column(BigInteger, nullable=True)


class ExtraUsers(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "extra"}

    username: Optional[str] = Column(String(255), nullable=True)
    password: Optional[str] = Column(String(255), nullable=True)
    user_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    role: Optional[str] = Column(String(50), nullable=True)
    name: Optional[str] = Column(String(255), nullable=True)

