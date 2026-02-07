from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, BigInteger, Integer, SmallInteger, String, Text
from .base import Base

class ImeImeCat(Base):
    __tablename__ = "ime_cat"
    __table_args__ = {"schema": "ime"}

    cat1: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    cat1_name: Optional[str] = Column(String(50), nullable=True)
    cat2: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    cat2_name: Optional[str] = Column(String(50), nullable=True)
    cat3: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    cat3_name: Optional[str] = Column(String(50), nullable=True)

class ImeImeDataHistorical(Base):
    __tablename__ = "ime_data_historical"
    __table_args__ = {"schema": "ime"}

    GoodsName: Optional[str] = Column(Text, nullable=True)
    Symbol: Optional[str] = Column(String(50), nullable=True)
    ProducerName: Optional[str] = Column(Text, nullable=True)
    ContractType: Optional[str] = Column(String(50), nullable=True)
    MinPrice: Optional[int] = Column(BigInteger, nullable=True)
    Price: Optional[int] = Column(BigInteger, nullable=True)
    MaxPrice: Optional[int] = Column(BigInteger, nullable=True)
    arze: Optional[int] = Column(Integer, nullable=True)
    ArzeBasePrice: Optional[int] = Column(BigInteger, nullable=True)
    arzeMinPrice: Optional[int] = Column(Integer, nullable=True)
    taghaza: Optional[int] = Column(Integer, nullable=True)
    taghazavoroudi: Optional[int] = Column(Integer, nullable=True)
    taghazaMaxPrice: Optional[int] = Column(BigInteger, nullable=True)
    Quantity: Optional[int] = Column(Integer, nullable=True)
    TotalPrice: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    DeliveryDate: Optional[str] = Column(String(50), nullable=True)
    Warehouse: Optional[str] = Column(Text, nullable=True)
    ArzehKonandeh: Optional[str] = Column(Text, nullable=True)
    SettlementDate: Optional[str] = Column(String(50), nullable=True)
    Category: Optional[str] = Column(String(50), nullable=True)
    xTalarReportPK: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    bArzehRadifTarSarresid: Optional[str] = Column(String(50), nullable=True)
    cBrokerSpcName: Optional[str] = Column(Text, nullable=True)
    ModeDescription: Optional[str] = Column(String(50), nullable=True)
    MethodDescription: Optional[str] = Column(Text, nullable=True)
    MinPrice1: Optional[int] = Column(BigInteger, nullable=True)
    Price1: Optional[int] = Column(BigInteger, nullable=True)
    Currency: Optional[str] = Column(String(50), nullable=True)
    Unit: Optional[str] = Column(String(50), nullable=True)
    arzehPk: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    Talar: Optional[str] = Column(String(50), nullable=True)
    PacketName: Optional[str] = Column(String(50), nullable=True)
    Tasvieh: Optional[str] = Column(String(50), nullable=True)

class ImeImeDataToday(Base):
    __tablename__ = "ime_data_today"
    __table_args__ = {"schema": "ime"}

    GoodsName: Optional[str] = Column(Text, nullable=True)
    Symbol: Optional[str] = Column(String(50), nullable=True)
    ProducerName: Optional[str] = Column(Text, nullable=True)
    ContractType: Optional[str] = Column(String(50), nullable=True)
    MinPrice: Optional[int] = Column(BigInteger, nullable=True)
    Price: Optional[int] = Column(BigInteger, nullable=True)
    MaxPrice: Optional[int] = Column(BigInteger, nullable=True)
    arze: Optional[int] = Column(Integer, nullable=True)
    ArzeBasePrice: Optional[int] = Column(BigInteger, nullable=True)
    arzeMinPrice: Optional[int] = Column(Integer, nullable=True)
    taghaza: Optional[int] = Column(Integer, nullable=True)
    taghazavoroudi: Optional[int] = Column(Integer, nullable=True)
    taghazaMaxPrice: Optional[int] = Column(BigInteger, nullable=True)
    Quantity: Optional[int] = Column(Integer, nullable=True)
    TotalPrice: Optional[int] = Column(BigInteger, nullable=True)
    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    DeliveryDate: Optional[str] = Column(String(50), nullable=True)
    Warehouse: Optional[str] = Column(Text, nullable=True)
    ArzehKonandeh: Optional[str] = Column(Text, nullable=True)
    SettlementDate: Optional[str] = Column(String(50), nullable=True)
    Category: Optional[str] = Column(String(50), nullable=True)
    xTalarReportPK: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    bArzehRadifTarSarresid: Optional[str] = Column(String(50), nullable=True)
    cBrokerSpcName: Optional[str] = Column(Text, nullable=True)
    ModeDescription: Optional[str] = Column(String(50), nullable=True)
    MethodDescription: Optional[str] = Column(Text, nullable=True)
    MinPrice1: Optional[int] = Column(BigInteger, nullable=True)
    Price1: Optional[int] = Column(BigInteger, nullable=True)
    Currency: Optional[str] = Column(String(50), nullable=True)
    Unit: Optional[str] = Column(String(50), nullable=True)
    arzehPk: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    Talar: Optional[str] = Column(String(50), nullable=True)
    PacketName: Optional[str] = Column(String(50), nullable=True)
    Tasvieh: Optional[str] = Column(String(50), nullable=True)
