from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, Float, Integer, String, Text
from .base import Base


class CommodityCommoditiesData(Base):
    __tablename__ = "commodities_data"
    __table_args__ = {"schema": "commodity"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    date_jalali: Optional[str] = Column(String(50), nullable=True)
    price: Optional[float] = Column(Float, nullable=True)
    owner: Optional[str] = Column(String(50), nullable=True)
    commodity: Optional[str] = Column(Text, nullable=True)
    unit: Optional[str] = Column(String(50), nullable=True)
    reference: Optional[str] = Column(Text, nullable=True)
    name: Optional[str] = Column(Text, nullable=False, primary_key=True)


class CommodityCommoditiesDetailData(Base):
    __tablename__ = "commodities_detail_data"
    __table_args__ = {"schema": "commodity"}

    owner: Optional[str] = Column(Text, nullable=True)
    id: Optional[str] = Column(Text, nullable=True)
    commodity: Optional[str] = Column(Text, nullable=True)
    commodity_id: Optional[str] = Column(Text, nullable=False, primary_key=True)
    unit: Optional[str] = Column(Text, nullable=True)
    reference: Optional[str] = Column(Text, nullable=True)
    retrieve_slug: Optional[str] = Column(Text, nullable=True)
    name: Optional[str] = Column(Text, nullable=True)
    result_sheet: Optional[str] = Column(Text, nullable=False, primary_key=True)
    result_name: Optional[str] = Column(Text, nullable=False, primary_key=True)


class CommodityCommoditiesItpnewsDetailData(Base):
    __tablename__ = "commodities_itpnews_detail_data"
    __table_args__ = {"schema": "commodity"}

    commodity: Optional[str] = Column(String(155), nullable=True)
    id: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class CommodityCommodityData(Base):
    __tablename__ = "commodity_data"
    __table_args__ = {"schema": "commodity"}

    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    current_price: Optional[float] = Column(Float, nullable=True)
    high_price: Optional[float] = Column(Float, nullable=True)
    open_price: Optional[float] = Column(Float, nullable=True)
    low_price: Optional[float] = Column(Float, nullable=True)
    td: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    date: Optional[str] = Column(String(255), nullable=True)
    time: Optional[str] = Column(String(255), nullable=True)


class CommodityCommodityDataToday(Base):
    __tablename__ = "commodity_data_today"
    __table_args__ = {"schema": "commodity"}

    name: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    id: Optional[int] = Column(Integer, nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    price: Optional[float] = Column(Float, nullable=True)
    price_change: Optional[float] = Column(Float, nullable=True)
    change_percent: Optional[float] = Column(Float, nullable=True)
    price_week: Optional[float] = Column(Float, nullable=True)
    price_one_month: Optional[float] = Column(Float, nullable=True)
    price_three_month: Optional[float] = Column(Float, nullable=True)
    price_six_month: Optional[float] = Column(Float, nullable=True)
    price_nine_month: Optional[float] = Column(Float, nullable=True)
    price_one_year: Optional[float] = Column(Float, nullable=True)
    price_start: Optional[float] = Column(Float, nullable=True)
