from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, Integer, SmallInteger, String, Float
from .base import Base


class KaladadeAssets(Base):
    __tablename__ = "assets"
    __table_args__ = {"schema": "kaladade"}

    cid2: Optional[int] = Column(Integer, nullable=True)
    cid3: Optional[int] = Column(Integer, nullable=True)
    cid4: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    name: Optional[str] = Column(String(150), nullable=True)
    prdi: Optional[int] = Column(Integer, nullable=True)
    rfci: Optional[int] = Column(Integer, nullable=True)
    alti: Optional[int] = Column(Integer, nullable=True)
    rt: Optional[int] = Column(Integer, nullable=True)
    un: Optional[int] = Column(Integer, nullable=True)
    cu1: Optional[int] = Column(Integer, nullable=True)
    cu2: Optional[int] = Column(Integer, nullable=True)
    cut: Optional[int] = Column(Integer, nullable=True)
    ila: Optional[int] = Column(SmallInteger, nullable=True)


class KaladadeCategories(Base):
    __tablename__ = "categories"
    __table_args__ = {"schema": "kaladade"}
    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    name: Optional[str] = Column(String(100), nullable=True)
    type: Optional[int] = Column(Integer, nullable=True)
    priority: Optional[int] = Column(Integer, nullable=True)


class KaladadeSubCategories(Base):
    __tablename__ = "sub_categories"
    __table_args__ = {"schema": "kaladade"}

    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    name: Optional[str] = Column(String(100), nullable=True)
    type: Optional[int] = Column(Integer, nullable=True)
    priority: Optional[int] = Column(Integer, nullable=True)
    category_id: Optional[int] = Column(Integer, nullable=True)


class KaladadeCurrencies(Base):
    __tablename__ = "currencies"
    __table_args__ = {"schema": "kaladade"}

    id: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    code: Optional[int] = Column(Integer, nullable=True)
    end_date_jalali: Optional[int] = Column(Integer, nullable=True)
    currency_name_en: Optional[str] = Column(String(100), nullable=True)
    currency_name_fa: Optional[str] = Column(String(100), nullable=True)
    price: Optional[float] = Column(Float, nullable=True)
    modified_at: Optional[str] = Column(String(50), nullable=True)
    time_elapsed: Optional[str] = Column(String(50), nullable=True)
    price_increase: Optional[float] = Column(Float, nullable=True)
    ticker: Optional[str] = Column(String(50), nullable=True)

