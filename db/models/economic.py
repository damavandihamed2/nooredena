from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, BigInteger, DateTime, Float, Text
from .base import Base


class EconomicGPRD(Base):
    __tablename__ = "GPRD"
    __table_args__ = {"schema": "economic"}

    DAY: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    N10D: Optional[int] = Column(BigInteger, nullable=True)
    GPRD: Optional[float] = Column(Float, nullable=True)
    GPRD_ACT: Optional[float] = Column(Float, nullable=True)
    GPRD_THREAT: Optional[float] = Column(Float, nullable=True)
    date: Optional[datetime] = Column(DateTime, nullable=True)
    GPRD_MA30: Optional[float] = Column(Float, nullable=True)
    GPRD_MA7: Optional[float] = Column(Float, nullable=True)
    event: Optional[str] = Column(Text, nullable=True)
    var_name: Optional[str] = Column(Text, nullable=True)
    var_label: Optional[str] = Column(Text, nullable=True)


class EconomicGPU(Base):
    __tablename__ = "GPU"
    __table_args__ = {"schema": "economic"}

    ym: Optional[str] = Column(Text, nullable=False, primary_key=True)
    GEPU_ppp: Optional[float] = Column(Float, nullable=True)


class EconomicCommoditiesAmarCentre(Base):
    __tablename__ = "commodities_amar_centre"
    __table_args__ = {"schema": "economic"}

    commodity: Optional[str] = Column(Text, nullable=False, primary_key=True)
    reference: Optional[str] = Column(Text, nullable=False, primary_key=True)
    price: Optional[float] = Column(Float, nullable=True)
    date_jalali: Optional[str] = Column(Text, nullable=True)
    date: Optional[str] = Column(Text, nullable=False, primary_key=True)
    owner: Optional[str] = Column(Text, nullable=True)
    unit: Optional[str] = Column(Text, nullable=True)


class EconomicForexfactoryMonthlyCalendar(Base):
    __tablename__ = "forexfactory_monthly_calendar"
    __table_args__ = {"schema": "economic"}

    year: Optional[str] = Column(Text, nullable=True)
    month: Optional[str] = Column(Text, nullable=True)
    date: Optional[str] = Column(Text, nullable=True)
    time: Optional[str] = Column(Text, nullable=True)
    currency: Optional[str] = Column(Text, nullable=False, primary_key=True)
    event: Optional[str] = Column(Text, nullable=False, primary_key=True)
    actual: Optional[str] = Column(Text, nullable=True)
    forecast: Optional[str] = Column(Text, nullable=True)
    previous: Optional[str] = Column(Text, nullable=True)
    actual_float: Optional[float] = Column(Float, nullable=True)
    forecast_float: Optional[float] = Column(Float, nullable=True)
    previous_float: Optional[float] = Column(Float, nullable=True)
    gdate: Optional[str] = Column(Text, nullable=False, primary_key=True)


class EconomicQuarterlyNationalAccounts(Base):
    __tablename__ = "quarterly_national_accounts"
    __table_args__ = {"schema": "economic"}

    year_season: Optional[str] = Column(Text, nullable=True)
    agriculture_group: Optional[float] = Column(Float, nullable=True)
    agriculture: Optional[float] = Column(Float, nullable=True)
    fishing: Optional[float] = Column(Float, nullable=True)
    industry_group: Optional[float] = Column(Float, nullable=True)
    mine: Optional[float] = Column(Float, nullable=True)
    oil_and_natural_gas_extraction: Optional[float] = Column(Float, nullable=True)
    other_mines: Optional[float] = Column(Float, nullable=True)
    industry: Optional[float] = Column(Float, nullable=True)
    natural_gas_distribution: Optional[float] = Column(Float, nullable=True)
    water_and_electricity_supply: Optional[float] = Column(Float, nullable=True)
    building: Optional[float] = Column(Float, nullable=True)
    service_group: Optional[float] = Column(Float, nullable=True)
    wholesale_and_retail_hotels_and_restaurants: Optional[float] = Column(Float, nullable=True)
    transportation_warehousing_and_communications: Optional[float] = Column(Float, nullable=True)
    financial_intermediation: Optional[float] = Column(Float, nullable=True)
    real_estate_rental_and_business_services: Optional[float] = Column(Float, nullable=True)
    public_affairs_education_health_and_welfare: Optional[float] = Column(Float, nullable=True)
    social_personal_and_domestic_services: Optional[float] = Column(Float, nullable=True)
    gross_domestic_product_at_basic_prices: Optional[float] = Column(Float, nullable=True)
    gross_domestic_product_excluding_oil: Optional[float] = Column(Float, nullable=True)
    season: Optional[str] = Column(Text, nullable=False, primary_key=True)
    seasonFa: Optional[str] = Column(Text, nullable=True)
    year: Optional[str] = Column(Text, nullable=False, primary_key=True)
    t: Optional[str] = Column(Text, nullable=True)
