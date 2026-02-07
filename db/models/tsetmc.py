from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, BigInteger, Float, Integer, Numeric, SmallInteger, String, Text
from .base import Base


class TsetmcDailyShareholdersChange(Base):
    __tablename__ = "daily_shareholders_change"
    __table_args__ = {"schema": "tsetmc"}

    jdate: Optional[str] = Column(Text, nullable=False, primary_key=True)
    name: Optional[str] = Column(Text, nullable=False, primary_key=True)
    change: Optional[float] = Column(Float, nullable=True)
    shareHolderName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    numberOfShares: Optional[float] = Column(Float, nullable=True)
    perOfShares: Optional[float] = Column(Float, nullable=True)
    beforeChange: Optional[float] = Column(Float, nullable=True)
    changeAmount: Optional[float] = Column(Float, nullable=True)
    changePer: Optional[float] = Column(Float, nullable=True)
    relativeChangePer: Optional[float] = Column(Float, nullable=True)


class TsetmcFundsReservesChange(Base):
    __tablename__ = "funds_reserves_change"
    __table_args__ = {"schema": "tsetmc"}

    jdate: Optional[str] = Column(Text, nullable=False, primary_key=True)
    name: Optional[str] = Column(Text, nullable=False, primary_key=True)
    change: Optional[float] = Column(Float, nullable=True)
    shareHolderName: Optional[str] = Column(Text, nullable=True)
    numberOfShares: Optional[float] = Column(Float, nullable=True)
    perOfShares: Optional[float] = Column(Float, nullable=True)
    beforeChange: Optional[float] = Column(Float, nullable=True)
    changeAmount: Optional[float] = Column(Float, nullable=True)
    changePer: Optional[float] = Column(Float, nullable=True)
    relativeChangePer: Optional[float] = Column(Float, nullable=True)


class TsetmcFundsShareholdingChange(Base):
    __tablename__ = "funds_shareholding_change"
    __table_args__ = {"schema": "tsetmc"}

    jdate: Optional[str] = Column(Text, nullable=False, primary_key=True)
    name: Optional[str] = Column(Text, nullable=False, primary_key=True)
    change: Optional[float] = Column(Float, nullable=True)
    shareHolderName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    numberOfShares: Optional[float] = Column(Float, nullable=True)
    perOfShares: Optional[float] = Column(Float, nullable=True)
    beforeChange: Optional[float] = Column(Float, nullable=True)
    changeAmount: Optional[float] = Column(Float, nullable=True)
    changePer: Optional[float] = Column(Float, nullable=True)
    relativeChangePer: Optional[float] = Column(Float, nullable=True)


class TsetmcIndices(Base):
    __tablename__ = "indices"
    __table_args__ = {"schema": "tsetmc"}

    indices: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    indices_name: Optional[str] = Column(String(255), nullable=True)
    indices_id: Optional[str] = Column(String(50), nullable=True)


class TsetmcIndicesData(Base):
    __tablename__ = "indices_data"
    __table_args__ = {"schema": "tsetmc"}

    index: Optional[str] = Column("index_", Text, nullable=False, primary_key=True)
    publication_time: Optional[str] = Column(String(50), nullable=True)
    last: Optional[int] = Column(Numeric, nullable=True)
    change: Optional[int] = Column(Numeric, nullable=True)
    change_percent: Optional[int] = Column(Numeric, nullable=True)
    high: Optional[int] = Column(Numeric, nullable=True)
    low: Optional[int] = Column(Numeric, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class TsetmcIndicesDataToday(Base):
    __tablename__ = "indices_data_today"
    __table_args__ = {"schema": "tsetmc"}

    indices_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    heven: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[float] = Column(Float, nullable=True)
    high_price: Optional[float] = Column(Float, nullable=True)
    low_price: Optional[float] = Column(Float, nullable=True)
    yesterday_price: Optional[float] = Column(Float, nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)


class TsetmcIndicesHistory(Base):
    __tablename__ = "indices_history"
    __table_args__ = {"schema": "tsetmc"}

    indices_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    close_price: Optional[float] = Column(Float, nullable=True)
    low_price: Optional[float] = Column(Float, nullable=True)
    high_price: Optional[float] = Column(Float, nullable=True)


class TsetmcIndicesReturn(Base):
    __tablename__ = "indices_return"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[str] = Column(String(50), nullable=True)
    indices_id: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    close_price: Optional[float] = Column(Float, nullable=True)
    price_one_week: Optional[float] = Column(Float, nullable=True)
    price_one_month: Optional[float] = Column(Float, nullable=True)
    price_two_month: Optional[float] = Column(Float, nullable=True)
    price_three_month: Optional[float] = Column(Float, nullable=True)
    price_six_month: Optional[float] = Column(Float, nullable=True)
    price_nine_month: Optional[float] = Column(Float, nullable=True)
    price_one_year: Optional[float] = Column(Float, nullable=True)
    price_two_year: Optional[float] = Column(Float, nullable=True)
    price_three_year: Optional[float] = Column(Float, nullable=True)
    price_five_year: Optional[float] = Column(Float, nullable=True)
    price_ten_year: Optional[float] = Column(Float, nullable=True)
    price_fiscal_year: Optional[float] = Column(Float, nullable=True)
    price_high_99: Optional[float] = Column(Float, nullable=True)
    price_high_02: Optional[float] = Column(Float, nullable=True)
    price_min_aban_03: Optional[float] = Column(Float, nullable=True)


class TsetmcMarketData(Base):
    __tablename__ = "market_data"
    __table_args__ = {"schema": "tsetmc"}

    total_index: Optional[float] = Column(Float, nullable=True)
    total_index_equal: Optional[float] = Column(Float, nullable=True)
    market_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(255), nullable=False, primary_key=True)


class TsetmcMarketDataDaily(Base):
    __tablename__ = "market_data_daily"
    __table_args__ = {"schema": "tsetmc"}

    total_index: Optional[float] = Column(Float, nullable=True)
    total_index_equal: Optional[float] = Column(Float, nullable=True)
    market_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(255), nullable=True)


class TsetmcMarketDataToday(Base):
    __tablename__ = "market_data_today"
    __table_args__ = {"schema": "tsetmc"}

    total_index: Optional[float] = Column(Float, nullable=True)
    total_index_equal: Optional[float] = Column(Float, nullable=True)
    market_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    total_index_change: Optional[int] = Column(Integer, nullable=True)
    total_index_equal_change: Optional[int] = Column(Integer, nullable=True)
    market_state: Optional[str] = Column(String(50), nullable=True)
    market_state_title: Optional[str] = Column(String(50), nullable=True)
    market_activity_date: Optional[str] = Column(String(50), nullable=True)
    market_activity_time: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(255), nullable=True)


class TsetmcMarketReturn(Base):
    __tablename__ = "market_return"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[str] = Column(String(10), nullable=True)
    symbol: Optional[str] = Column(String(255), nullable=True)
    symbol_id: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    total_share: Optional[int] = Column(BigInteger, nullable=True)
    sector: Optional[str] = Column(String(50), nullable=True)
    price_last: Optional[int] = Column(Integer, nullable=True)
    price_week: Optional[int] = Column(Integer, nullable=True)
    price_one_month: Optional[int] = Column(Integer, nullable=True)
    price_two_month: Optional[int] = Column(Integer, nullable=True)
    price_three_month: Optional[int] = Column(Integer, nullable=True)
    price_six_month: Optional[int] = Column(Integer, nullable=True)
    price_nine_month: Optional[int] = Column(Integer, nullable=True)
    price_year: Optional[int] = Column(Integer, nullable=True)
    price_fiscal_year: Optional[int] = Column(Integer, nullable=True)
    price_two_year: Optional[int] = Column(Integer, nullable=True)
    price_three_year: Optional[int] = Column(Integer, nullable=True)
    price_five_year: Optional[int] = Column(Integer, nullable=True)
    price_ten_year: Optional[int] = Column(Integer, nullable=True)
    price_high_99: Optional[int] = Column(Integer, nullable=True)
    price_high_02: Optional[int] = Column(Integer, nullable=True)
    price_min_aban_03: Optional[int] = Column(Integer, nullable=True)


class TsetmcOptionsDataDaily(Base):
    __tablename__ = "options_data_daily"
    __table_args__ = {"schema": "tsetmc"}

    base_symbol: Optional[str] = Column(String(50), nullable=True)
    base_symbol_id: Optional[str] = Column(String(50), nullable=True)
    contract_size: Optional[int] = Column(Integer, nullable=True)
    strike_price: Optional[int] = Column(Integer, nullable=True)
    begin_date: Optional[str] = Column(String(50), nullable=True)
    end_date: Optional[str] = Column(String(50), nullable=True)
    remained_day: Optional[int] = Column(Integer, nullable=True)
    base_close_price: Optional[int] = Column(Integer, nullable=True)
    base_last_price: Optional[int] = Column(Integer, nullable=True)
    base_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    call_symbol: Optional[str] = Column(String(50), nullable=True)
    call_symbol_name: Optional[str] = Column(String(255), nullable=True)
    call_symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    call_open_position: Optional[int] = Column(Integer, nullable=True)
    call_yesterday_open_position: Optional[int] = Column(Integer, nullable=True)
    call_close_price: Optional[int] = Column(Integer, nullable=True)
    call_last_price: Optional[int] = Column(Integer, nullable=True)
    call_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    call_national_value: Optional[int] = Column(BigInteger, nullable=True)
    call_trade_amount: Optional[int] = Column(Integer, nullable=True)
    call_trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    call_trade_value: Optional[int] = Column(BigInteger, nullable=True)
    call_buy_price: Optional[int] = Column(Integer, nullable=True)
    call_buy_volume: Optional[int] = Column(Integer, nullable=True)
    call_sell_price: Optional[int] = Column(Integer, nullable=True)
    call_sell_volume: Optional[int] = Column(Integer, nullable=True)
    put_symbol: Optional[str] = Column(String(50), nullable=True)
    put_symbol_name: Optional[str] = Column(String(255), nullable=True)
    put_symbol_id: Optional[str] = Column(String(50), nullable=True)
    put_open_position: Optional[int] = Column(Integer, nullable=True)
    put_yesterday_open_position: Optional[int] = Column(Integer, nullable=True)
    put_close_price: Optional[int] = Column(Integer, nullable=True)
    put_last_price: Optional[int] = Column(Integer, nullable=True)
    put_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    put_national_value: Optional[int] = Column(BigInteger, nullable=True)
    put_trade_amount: Optional[int] = Column(Integer, nullable=True)
    put_trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    put_trade_value: Optional[int] = Column(BigInteger, nullable=True)
    put_buy_price: Optional[int] = Column(Integer, nullable=True)
    put_buy_volume: Optional[int] = Column(Integer, nullable=True)
    put_sell_price: Optional[int] = Column(Integer, nullable=True)
    put_sell_volume: Optional[int] = Column(Integer, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class TsetmcOptionsDataToday(Base):
    __tablename__ = "options_data_today"
    __table_args__ = {"schema": "tsetmc"}

    base_symbol: Optional[str] = Column(String(50), nullable=True)
    base_symbol_id: Optional[str] = Column(String(50), nullable=True)
    contract_size: Optional[int] = Column(Integer, nullable=True)
    strike_price: Optional[int] = Column(Integer, nullable=True)
    begin_date: Optional[str] = Column(String(50), nullable=True)
    end_date: Optional[str] = Column(String(50), nullable=True)
    remained_day: Optional[int] = Column(Integer, nullable=True)
    base_close_price: Optional[int] = Column(Integer, nullable=True)
    base_last_price: Optional[int] = Column(Integer, nullable=True)
    base_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    call_symbol: Optional[str] = Column(String(50), nullable=True)
    call_symbol_name: Optional[str] = Column(String(255), nullable=True)
    call_symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    call_open_position: Optional[int] = Column(Integer, nullable=True)
    call_yesterday_open_position: Optional[int] = Column(Integer, nullable=True)
    call_close_price: Optional[int] = Column(Integer, nullable=True)
    call_last_price: Optional[int] = Column(Integer, nullable=True)
    call_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    call_national_value: Optional[int] = Column(BigInteger, nullable=True)
    call_trade_amount: Optional[int] = Column(Integer, nullable=True)
    call_trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    call_trade_value: Optional[int] = Column(BigInteger, nullable=True)
    call_buy_price: Optional[int] = Column(Integer, nullable=True)
    call_buy_volume: Optional[int] = Column(Integer, nullable=True)
    call_sell_price: Optional[int] = Column(Integer, nullable=True)
    call_sell_volume: Optional[int] = Column(Integer, nullable=True)
    put_symbol: Optional[str] = Column(String(50), nullable=True)
    put_symbol_name: Optional[str] = Column(String(255), nullable=True)
    put_symbol_id: Optional[str] = Column(String(50), nullable=True)
    put_open_position: Optional[int] = Column(Integer, nullable=True)
    put_yesterday_open_position: Optional[int] = Column(Integer, nullable=True)
    put_close_price: Optional[int] = Column(Integer, nullable=True)
    put_last_price: Optional[int] = Column(Integer, nullable=True)
    put_yesterday_price: Optional[int] = Column(Integer, nullable=True)
    put_national_value: Optional[int] = Column(BigInteger, nullable=True)
    put_trade_amount: Optional[int] = Column(Integer, nullable=True)
    put_trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    put_trade_value: Optional[int] = Column(BigInteger, nullable=True)
    put_buy_price: Optional[int] = Column(Integer, nullable=True)
    put_buy_volume: Optional[int] = Column(Integer, nullable=True)
    put_sell_price: Optional[int] = Column(Integer, nullable=True)
    put_sell_volume: Optional[int] = Column(Integer, nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)


class TsetmcRawShareholders(Base):
    __tablename__ = "raw_shareholders"
    __table_args__ = {"schema": "tsetmc"}

    shareHolderID: Optional[float] = Column(Float, nullable=True)
    shareHolderName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    cIsin: Optional[str] = Column(Text, nullable=True)
    dEven: Optional[float] = Column(Float, nullable=True)
    numberOfShares: Optional[float] = Column(Float, nullable=True)
    perOfShares: Optional[float] = Column(Float, nullable=True)
    change: Optional[float] = Column(Float, nullable=False, primary_key=True)
    changeAmount: Optional[float] = Column(Float, nullable=True)
    shareHolderShareID: Optional[float] = Column(Float, nullable=True)
    name: Optional[str] = Column(Text, nullable=True)
    sharesno: Optional[float] = Column(Float, nullable=True)
    code: Optional[str] = Column(Text, nullable=False, primary_key=True)
    jdate: Optional[str] = Column(Text, nullable=False, primary_key=True)


class TsetmcSectorDetailData(Base):
    __tablename__ = "sector_detail_data"
    __table_args__ = {"schema": "tsetmc"}

    sector: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    market_value: int = Column(BigInteger, nullable=False)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class TsetmcSectorDetailDataDaily(Base):
    __tablename__ = "sector_detail_data_daily"
    __table_args__ = {"schema": "tsetmc"}

    sector: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    market_value: int = Column(BigInteger, nullable=False)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(50), nullable=True)


class TsetmcSectorDetailDataToday(Base):
    __tablename__ = "sector_detail_data_today"
    __table_args__ = {"schema": "tsetmc"}

    sector: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    market_value: int = Column(BigInteger, nullable=False)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)


class TsetmcSectorHistoricalData(Base):
    __tablename__ = "sector_historical_data"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    sector: Optional[str] = Column(String(50), nullable=True)
    sector_id: Optional[str] = Column(Text, nullable=False, primary_key=True)
    final: Optional[float] = Column(Float, nullable=True)
    high: Optional[float] = Column(Float, nullable=True)
    low: Optional[float] = Column(Float, nullable=True)


class TsetmcStockHistoricalData(Base):
    __tablename__ = "stock_historical_data"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(Text, nullable=True)
    symbol_name: Optional[str] = Column(Text, nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    open_price: Optional[int] = Column(BigInteger, nullable=True)
    high_price: Optional[int] = Column(BigInteger, nullable=True)
    low_price: Optional[int] = Column(BigInteger, nullable=True)
    close_price: Optional[int] = Column(BigInteger, nullable=True)
    final_price: Optional[int] = Column(BigInteger, nullable=True)
    yesterday_price: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    trade_amount: Optional[int] = Column(BigInteger, nullable=True)


class TsetmcSymbolBeta(Base):
    __tablename__ = "symbol_beta"
    __table_args__ = {"schema": "tsetmc"}

    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    beta: Optional[float] = Column(Float, nullable=True)


class TsetmcSymbols(Base):
    __tablename__ = "symbols"
    __table_args__ = {"schema": "tsetmc"}

    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    symbol_name_eng: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_id_2: Optional[str] = Column(String(50), nullable=True)
    symbol_id_3: Optional[str] = Column(String(50), nullable=True)
    symbol_id_4: Optional[str] = Column(String(50), nullable=True)
    symbol_eng: Optional[str] = Column(String(50), nullable=True)
    symbol_code: Optional[str] = Column(String(50), nullable=True)
    company: Optional[str] = Column(String(50), nullable=True)
    company_code: Optional[str] = Column(String(50), nullable=True)
    sector: Optional[str] = Column(String(50), nullable=True)
    sector_name: Optional[str] = Column(String(255), nullable=True)
    subsector: Optional[str] = Column(String(50), nullable=True)
    subsector_name: Optional[str] = Column(String(255), nullable=True)
    flow: Optional[int] = Column(SmallInteger, nullable=True)
    flow_name: Optional[str] = Column(String(50), nullable=True)
    yval: Optional[str] = Column(String(50), nullable=True)
    description: Optional[str] = Column(Text, nullable=True)
    total_share: Optional[int] = Column(BigInteger, nullable=True)
    floating_share: Optional[int] = Column(Integer, nullable=True)
    base_volume: Optional[int] = Column(BigInteger, nullable=True)
    monthly_mean_volume: Optional[int] = Column(BigInteger, nullable=True)
    top_symbol: Optional[int] = Column(SmallInteger, nullable=True)
    estimated_eps: Optional[int] = Column(BigInteger, nullable=True)
    psr: Optional[float] = Column(Float, nullable=True)
    sector_pe: Optional[float] = Column(Float, nullable=True)
    nav: Optional[int] = Column(Integer, nullable=True)
    min_week: Optional[int] = Column(Integer, nullable=True)
    max_week: Optional[int] = Column(Integer, nullable=True)
    min_year: Optional[int] = Column(Integer, nullable=True)
    max_year: Optional[int] = Column(Integer, nullable=True)
    open_price: Optional[int] = Column(Integer, nullable=True)
    high_price: Optional[int] = Column(Integer, nullable=True)
    low_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    change_price: Optional[int] = Column(Integer, nullable=True)
    trade_amount: Optional[int] = Column(Integer, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    under_supervision: Optional[int] = Column(SmallInteger, nullable=True)
    status: Optional[str] = Column(String(50), nullable=True)
    status_name: Optional[str] = Column(String(50), nullable=True)
    board: Optional[int] = Column(SmallInteger, nullable=True)
    sttc_thr_max: Optional[int] = Column(Integer, nullable=True)
    sttc_thr_min: Optional[int] = Column(Integer, nullable=True)
    last_date: Optional[int] = Column(Integer, nullable=True)
    last_time: Optional[int] = Column(Integer, nullable=True)
    final_last_date: Optional[int] = Column(Integer, nullable=True)
    category: Optional[str] = Column(String(50), nullable=True)
    category_name: Optional[str] = Column(String(255), nullable=True)
    last: Optional[str] = Column(String(50), nullable=True)
    iclose: Optional[str] = Column(String(50), nullable=True)
    yclose: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column("date_", String(10), nullable=True)
    time: Optional[str] = Column("time_", String(10), nullable=True)
    active: Optional[int] = Column(SmallInteger, nullable=True)
    fund_units_issued: Optional[int] = Column(BigInteger, nullable=True)
    fund_units_date: Optional[int] = Column(Integer, nullable=True)


class TsetmcSymbolsClienttype(Base):
    __tablename__ = "symbols_clienttype"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    buy_i_count: Optional[int] = Column(Integer, nullable=True)
    buy_i_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_i_value: Optional[int] = Column(BigInteger, nullable=True)
    buy_n_count: Optional[int] = Column(Integer, nullable=True)
    buy_n_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_n_value: Optional[int] = Column(BigInteger, nullable=True)
    sell_i_count: Optional[int] = Column(Integer, nullable=True)
    sell_i_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_i_value: Optional[int] = Column(BigInteger, nullable=True)
    sell_n_count: Optional[int] = Column(Integer, nullable=True)
    sell_n_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_n_value: Optional[int] = Column(BigInteger, nullable=True)


class TsetmcSymbolsData(Base):
    __tablename__ = "symbols_data"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    symbol_name_eng: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(255), nullable=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    heven: Optional[int] = Column(Integer, nullable=True)
    first_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)
    trade_number: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    day_low: Optional[int] = Column(Integer, nullable=True)
    day_high: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    eps: Optional[int] = Column(Integer, nullable=True)
    base_volume: Optional[int] = Column(BigInteger, nullable=True)
    visit_count: Optional[int] = Column(Integer, nullable=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    sector: Optional[str] = Column(String(255), nullable=True)
    day_max: Optional[int] = Column(BigInteger, nullable=True)
    day_min: Optional[int] = Column(Integer, nullable=True)
    share_number: Optional[int] = Column(BigInteger, nullable=True)
    yval: Optional[int] = Column(Integer, nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    open_position: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    category: Optional[str] = Column(String(10), nullable=True)


class TsetmcSymbolsDataDaily(Base):
    __tablename__ = "symbols_data_daily"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    symbol_name_eng: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(255), nullable=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    heven: Optional[int] = Column(Integer, nullable=True)
    first_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)
    trade_number: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    day_low: Optional[int] = Column(Integer, nullable=True)
    day_high: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    eps: Optional[int] = Column(Integer, nullable=True)
    base_volume: Optional[int] = Column(BigInteger, nullable=True)
    visit_count: Optional[int] = Column(Integer, nullable=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    sector: Optional[str] = Column(String(255), nullable=True)
    day_max: Optional[int] = Column(BigInteger, nullable=True)
    day_min: Optional[int] = Column(Integer, nullable=True)
    share_number: Optional[int] = Column(BigInteger, nullable=True)
    yval: Optional[int] = Column(Integer, nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    open_position: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    time: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    category: Optional[str] = Column(String(10), nullable=True)


class TsetmcSymbolsDataToday(Base):
    __tablename__ = "symbols_data_today"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_name_eng: Optional[str] = Column(String(50), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_name: Optional[str] = Column(String(255), nullable=True)
    heven: Optional[int] = Column(Integer, nullable=True)
    first_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)
    trade_number: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    day_low: Optional[int] = Column(Integer, nullable=True)
    day_high: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    eps: Optional[int] = Column(Integer, nullable=True)
    base_volume: Optional[int] = Column(BigInteger, nullable=True)
    visit_count: Optional[int] = Column(Integer, nullable=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    sector: Optional[str] = Column(String(255), nullable=True)
    day_max: Optional[int] = Column(BigInteger, nullable=True)
    day_min: Optional[int] = Column(Integer, nullable=True)
    share_number: Optional[int] = Column(BigInteger, nullable=True)
    yval: Optional[int] = Column(Integer, nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    open_position: Optional[int] = Column(BigInteger, nullable=True)
    close_change: Optional[float] = Column(Float, nullable=True)
    last_change: Optional[float] = Column(Float, nullable=True)
    sell_count: Optional[int] = Column(Integer, nullable=True)
    buy_count: Optional[int] = Column(Integer, nullable=True)
    buy_price: Optional[int] = Column(Integer, nullable=True)
    sell_price: Optional[int] = Column(Integer, nullable=True)
    buy_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_number_natural: Optional[int] = Column(Integer, nullable=True)
    buy_number_legal: Optional[int] = Column(Integer, nullable=True)
    buy_volume_natural: Optional[int] = Column(BigInteger, nullable=True)
    buy_volume_legal: Optional[int] = Column(BigInteger, nullable=True)
    sell_number_natural: Optional[int] = Column(Integer, nullable=True)
    sell_number_legal: Optional[int] = Column(Integer, nullable=True)
    sell_volume_natural: Optional[int] = Column(BigInteger, nullable=True)
    sell_volume_legal: Optional[int] = Column(BigInteger, nullable=True)
    money_in_out_natural: Optional[int] = Column(BigInteger, nullable=True)
    mean_buy_natural: Optional[int] = Column(BigInteger, nullable=True)
    mean_sell_natural: Optional[int] = Column(BigInteger, nullable=True)
    buyer_to_seller_power: Optional[float] = Column(Float, nullable=True)
    block: Optional[int] = Column(SmallInteger, nullable=True)
    date: Optional[str] = Column(String(255), nullable=True)
    time: Optional[str] = Column(String(255), nullable=True)
    category: Optional[str] = Column(String(10), nullable=True)


class TsetmcSymbolsDetailData(Base):
    __tablename__ = "symbols_detail_data"
    __table_args__ = {"schema": "tsetmc"}

    symbol: Optional[str] = Column(Text, nullable=True)
    symbol_name: Optional[str] = Column(Text, nullable=True)
    symbol_eng: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    sector: Optional[str] = Column(String(50), nullable=True)
    sector_name: Optional[str] = Column(Text, nullable=True)
    total_share: Optional[int] = Column(BigInteger, nullable=True)
    flow: Optional[int] = Column(Integer, nullable=True)
    flow_title: Optional[str] = Column(Text, nullable=True)
    sector_id: Optional[str] = Column(String(50), nullable=True)
    sub_sector: Optional[str] = Column(String(50), nullable=True)
    sub_sector_name: Optional[str] = Column(Text, nullable=True)
    last_price: Optional[int] = Column(Integer, nullable=True)


class TsetmcSymbolsDividend(Base):
    __tablename__ = "symbols_dividend"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    price_old: Optional[int] = Column(Integer, nullable=True)
    price_new: Optional[int] = Column(Integer, nullable=True)
    is_adjusted: Optional[int] = Column(Integer, nullable=True)


class TsetmcSymbolsHistory(Base):
    __tablename__ = "symbols_history"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    open_price: Optional[int] = Column(Integer, nullable=True)
    high_price: Optional[int] = Column(Integer, nullable=True)
    low_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    trade_amount: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)


class TsetmcSymbolsRos(Base):
    __tablename__ = "symbols_ros"
    __table_args__ = {"schema": "tsetmc"}

    symbol_ros: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol_id_2: Optional[str] = Column(String(50), nullable=True)
    symbol_id_3: Optional[str] = Column(String(50), nullable=True)
    symbol_id_4: Optional[str] = Column(String(50), nullable=True)
    active: Optional[int] = Column(Integer, nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column("date_", String(50), nullable=True)
    time: Optional[str] = Column("time_", String(50), nullable=True)


class TsetmcSymbolsRosClienttype(Base):
    __tablename__ = "symbols_ros_clienttype"
    __table_args__ = {"schema": "tsetmc"}

    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date: Optional[int] = Column(Integer, nullable=True)
    buy_i_count: Optional[int] = Column(Integer, nullable=True)
    buy_i_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_i_value: Optional[int] = Column(BigInteger, nullable=True)
    buy_n_count: Optional[int] = Column(Integer, nullable=True)
    buy_n_volume: Optional[int] = Column(BigInteger, nullable=True)
    buy_n_value: Optional[int] = Column(BigInteger, nullable=True)
    sell_i_count: Optional[int] = Column(Integer, nullable=True)
    sell_i_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_i_value: Optional[int] = Column(BigInteger, nullable=True)
    sell_n_count: Optional[int] = Column(Integer, nullable=True)
    sell_n_volume: Optional[int] = Column(BigInteger, nullable=True)
    sell_n_value: Optional[int] = Column(BigInteger, nullable=True)


class TsetmcSymbolsRosHistory(Base):
    __tablename__ = "symbols_ros_history"
    __table_args__ = {"schema": "tsetmc"}

    date: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    open_price: Optional[int] = Column(Integer, nullable=True)
    high_price: Optional[int] = Column(Integer, nullable=True)
    low_price: Optional[int] = Column(Integer, nullable=True)
    close_price: Optional[int] = Column(Integer, nullable=True)
    yesterday_price: Optional[int] = Column(Integer, nullable=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    trade_amount: Optional[int] = Column(BigInteger, nullable=True)
    trade_volume: Optional[int] = Column(BigInteger, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
