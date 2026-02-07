from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, BigInteger, Boolean, DateTime, Float, String, Text
from .base import Base


class BourseviewBanksActivities(Base):
    __tablename__ = "banks_activities"
    __table_args__ = {"schema": "bourseview"}

    productionItemKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productionItemName: Optional[str] = Column(Text, nullable=True)
    productKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productName: Optional[str] = Column(Text, nullable=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    valueGrowthRate: Optional[float] = Column(Float, nullable=True)
    valueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    rowKey: Optional[str] = Column(Text, nullable=True)
    unitKey: Optional[int] = Column(BigInteger, nullable=True)
    unitName: Optional[str] = Column(Text, nullable=True)
    cleanProductId: Optional[int] = Column(BigInteger, nullable=True)
    cleanProductName: Optional[str] = Column(Text, nullable=True)
    fmDefaultValueQuarterly: Optional[float] = Column(Float, nullable=True)
    fmDefaultValue: Optional[float] = Column(Float, nullable=True)
    productionKey: Optional[int] = Column(BigInteger, nullable=True)
    staticRowName: Optional[str] = Column(Text, nullable=True)
    value: Optional[float] = Column(Float, nullable=True)
    display_order: Optional[int] = Column(BigInteger, nullable=True)
    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    stock: Optional[str] = Column(Text, nullable=True)
    code: Optional[str] = Column(Text, nullable=False, primary_key=True)


class BourseviewBanksActivitiesAgg(Base):
    __tablename__ = "banks_activities_agg"
    __table_args__ = {"schema": "bourseview"}

    stock: Optional[str] = Column(Text, nullable=False, primary_key=True)
    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productionItemName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    jdate: Optional[str] = Column(Text, nullable=True)
    jyear: Optional[int] = Column(BigInteger, nullable=True)
    annualValueOrigin: Optional[float] = Column(Float, nullable=True)
    valueOrigin_lag1: Optional[float] = Column(Float, nullable=True)
    growthToLastMonth: Optional[float] = Column(Float, nullable=True)
    annualValueOrigin_lag1: Optional[float] = Column(Float, nullable=True)
    growthToLastYear: Optional[float] = Column(Float, nullable=True)


class BourseviewBanksData(Base):
    __tablename__ = "banks_data"
    __table_args__ = {"schema": "bourseview"}

    bank: Optional[str] = Column(Text, nullable=False, primary_key=True)
    fiscalYear: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    fiscalMonth: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    facilities_t0_revise: Optional[float] = Column(Float, nullable=True)
    deposits_t0_revise: Optional[float] = Column(Float, nullable=True)
    facilities_t: Optional[float] = Column(Float, nullable=True)
    facilities_due_t: Optional[float] = Column(Float, nullable=True)
    deposits_net: Optional[float] = Column(Float, nullable=True)
    facilities_income: Optional[float] = Column(Float, nullable=True)
    operational_incomes: Optional[float] = Column(Float, nullable=True)
    deposits_due_t: Optional[float] = Column(Float, nullable=True)
    deposits_t: Optional[float] = Column(Float, nullable=True)
    investment_interest: Optional[float] = Column( Float, nullable=True)
    facilities_t0: Optional[float] = Column(Float, nullable=True)
    deposits_t0: Optional[float] = Column(Float, nullable=True)
    facilities_t1: Optional[float] = Column(Float, nullable=True)
    deposits_t1: Optional[float] = Column(Float, nullable=True)
    bond_investment: Optional[float] = Column(Float, nullable=True)
    stock_investment: Optional[float] = Column(Float, nullable=True)
    operational_expenses: Optional[float] = Column(Float, nullable=True)
    finance_expenses: Optional[float] = Column(Float, nullable=True)
    facilities_effective_rate: Optional[float] = Column(Float, nullable=True)
    deposits_effective_rate: Optional[float] = Column(Float, nullable=True)
    operational_spread: Optional[float] = Column(Float, nullable=True)


class BourseviewBanksDataRaw(Base):
    __tablename__ = "banks_data_raw"
    __table_args__ = {"schema": "bourseview"}

    productionItemKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productionItemName: Optional[str] = Column(Text, nullable=True)
    productKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productName: Optional[str] = Column(Text, nullable=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    valueGrowthRate: Optional[float] = Column(Float, nullable=True)
    valueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    rowKey: Optional[str] = Column(Text, nullable=True)
    unitKey: Optional[int] = Column(BigInteger, nullable=True)
    unitName: Optional[str] = Column(Text, nullable=True)
    cleanProductId: Optional[int] = Column(BigInteger, nullable=True)
    cleanProductName: Optional[str] = Column(Text, nullable=True)
    fmDefaultValueQuarterly: Optional[float] = Column(Float, nullable=True)
    fmDefaultValue: Optional[float] = Column(Float, nullable=True)
    productionKey: Optional[int] = Column(BigInteger, nullable=True)
    staticRowName: Optional[str] = Column(Text, nullable=True)
    value: Optional[float] = Column(Float, nullable=True)
    display_order: Optional[int] = Column(BigInteger, nullable=True)
    fiscalMonth: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    fiscalYear: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    bank: Optional[str] = Column(Text, nullable=True)
    code: Optional[str] = Column(Text, nullable=False, primary_key=True)


class BourseviewBourseviewIndustries(Base):
    __tablename__ = "bourseview_industries"
    __table_args__ = {"schema": "bourseview"}

    symbol_code: Optional[str] = Column(Text, nullable=False, primary_key=True)
    clean_symbol: Optional[str] = Column(Text, nullable=True)
    bourseview_industry: Optional[str] = Column(Text, nullable=True)
    bourseview_group: Optional[str] = Column(Text, nullable=True)
    ticker_key: Optional[int] = Column(BigInteger, nullable=True)


class BourseviewCompaniesProductionSell(Base):
    __tablename__ = "companies_production_sell"
    __table_args__ = {"schema": "bourseview"}

    productionItemKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productionItemName: Optional[str] = Column(Text, nullable=True)
    productKey: Optional[int] = Column(BigInteger, nullable=True)
    productName: Optional[str] = Column(Text, nullable=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    valueGrowthRate: Optional[float] = Column(Float, nullable=True)
    valueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    rowKey: Optional[float] = Column(Float, nullable=True)
    unitKey: Optional[float] = Column(Float, nullable=True)
    unitName: Optional[str] = Column(Text, nullable=True)
    cleanProductId: Optional[float] = Column(Float, nullable=True)
    cleanProductName: Optional[str] = Column(Text, nullable=True)
    fmDefaultValueQuarterly: Optional[float] = Column(Float, nullable=True)
    fmDefaultValue: Optional[float] = Column(Float, nullable=True)
    productionKey: Optional[float] = Column(Float, nullable=False, primary_key=True)
    staticRowName: Optional[str] = Column(Text, nullable=True)
    tableId: Optional[int] = Column(BigInteger, nullable=True)
    tableName: Optional[str] = Column(Text, nullable=True)
    display_order: Optional[int] = Column(BigInteger, nullable=True)
    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    value: Optional[float] = Column(Float, nullable=True)
    isTotal: Optional[bool] = Column(Boolean, nullable=True)
    stock: Optional[str] = Column(Text, nullable=True)
    code: Optional[str] = Column(Text, nullable=False, primary_key=True)


class BourseviewCompaniesProductionSellWithIndustry(Base):
    __tablename__ = "companies_production_sell_with_industry"
    __table_args__ = {"schema": "bourseview"}

    productionItemName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    productKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productName: Optional[str] = Column(Text, nullable=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    valueGrowthRate: Optional[float] = Column(Float, nullable=True)
    valueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    unitName: Optional[str] = Column(Text, nullable=True)
    tableId: Optional[int] = Column(BigInteger, nullable=True)
    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    isTotal: Optional[bool] = Column(Boolean, nullable=True)
    symbol_code: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    tse_sector: Optional[str] = Column(String(255), nullable=True)
    stock: Optional[str] = Column(Text, nullable=True)
    industry: Optional[str] = Column(Text, nullable=True)


class BourseviewCps(Base):
    __tablename__ = "cps"
    __table_args__ = {"schema": "bourseview"}

    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    stock: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    industry: Optional[str] = Column(Text, nullable=True)
    productKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    productName: Optional[str] = Column(String(255), nullable=True)
    tableId: Optional[int] = Column(BigInteger, nullable=True)
    productionItemName: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    valueGrowthRate: Optional[float] = Column(Float, nullable=True)
    valueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    unitName: Optional[str] = Column(String(255), nullable=True)
    jdate: Optional[str] = Column(String(255), nullable=True)
    jdate_int: Optional[int] = Column(BigInteger, nullable=True)
    aggValueOrigin: Optional[float] = Column(Float, nullable=True)
    aggValueGrowthRate: Optional[float] = Column(Float, nullable=True)
    aggValueGrowthRateYoy: Optional[float] = Column(Float, nullable=True)
    agg_total_rate: Optional[float] = Column(Float, nullable=True)
    valueOrigin_lag1: Optional[float] = Column(Float, nullable=True)
    valueOrigin_ma12: Optional[float] = Column(Float, nullable=True)
    aggValueOrigin_lag1: Optional[float] = Column(Float, nullable=True)
    aggValueOrigin_ma12: Optional[float] = Column(Float, nullable=True)
    rate_ma12_custom: Optional[float] = Column(Float, nullable=True)
    amount_sum12: Optional[float] = Column(Float, nullable=True)
    qty_sum12: Optional[float] = Column(Float, nullable=True)
    valueOrigin_ma12_custom: Optional[float] = Column(Float, nullable=True)
    isPicked: Optional[bool] = Column(Boolean, nullable=True)
    share12: Optional[float] = Column(Float, nullable=True)
    share12_pct: Optional[float] = Column(Float, nullable=True)
    cum_share12_pct: Optional[float] = Column(Float, nullable=True)
    picked_reason: Optional[str] = Column(String(255), nullable=True)
    rn: Optional[float] = Column(Float, nullable=True)
    rate12_display: Optional[float] = Column(Float, nullable=True)
    agg_total_display: Optional[float] = Column(Float, nullable=True)
    agg_growth_display: Optional[float] = Column(Float, nullable=True)
    agg_growth_rate_decimal: Optional[float] = Column(Float, nullable=True)
    meanDeviation: Optional[float] = Column(Float, nullable=True)


class BourseviewCompaniesIncomeQ(Base):
    __tablename__ = "companies_income_q"
    __table_args__ = {"schema": "bourseview"}

    periodEndingDate: Optional[datetime] = Column(DateTime, nullable=False, primary_key=True)
    auditing: Optional[str] = Column(Text, nullable=True)
    statementKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    fiscalMonth: Optional[int] = Column(BigInteger, nullable=True)
    statementItemName: Optional[str] = Column(Text, nullable=True)
    value: Optional[float] = Column(Float, nullable=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    isTotal: Optional[bool] = Column(Boolean, nullable=True)
    summaryLevel: Optional[int] = Column(BigInteger, nullable=True)
    statementItemKey: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    stock: Optional[str] = Column(Text, nullable=True)
    code: Optional[str] = Column(Text, nullable=False, primary_key=True)


class BourseviewCompaniesIncomeQWithIndustry(Base):
    __tablename__ = "companies_income_q_with_industry"
    __table_args__ = {"schema": "bourseview"}

    statementItemName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    valueOrigin: Optional[float] = Column(Float, nullable=True)
    valueVertical: Optional[float] = Column(Float, nullable=True)
    periodEndingDate: Optional[datetime] = Column(DateTime, nullable=False, primary_key=True)
    isTotal: Optional[bool] = Column(Boolean, nullable=True)
    auditing: Optional[str] = Column(Text, nullable=False, primary_key=True)
    fiscalMonth: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    symbol_code: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    tse_sector: Optional[str] = Column(String(255), nullable=True)
    stock: Optional[str] = Column(Text, nullable=True)
    industry: Optional[str] = Column(Text, nullable=True)


class BourseviewCompaniesPortfolioItems(Base):
    __tablename__ = "companies_portfolio_items"
    __table_args__ = {"schema": "bourseview"}

    month: Optional[str] = Column(Text, nullable=False, primary_key=True)
    company: Optional[str] = Column(Text, nullable=False, primary_key=True)
    companySymbol: Optional[str] = Column(Text, nullable=False, primary_key=True)
    per_total: Optional[float] = Column(Float, nullable=True)


class BourseviewCompaniesTransactions(Base):
    __tablename__ = "companies_transactions"
    __table_args__ = {"schema": "bourseview"}

    month: Optional[str] = Column(Text, nullable=False, primary_key=True)
    company: Optional[str] = Column(Text, nullable=False, primary_key=True)
    companySymbol: Optional[str] = Column(Text, nullable=False, primary_key=True)
    type: Optional[str] = Column(Text, nullable=False, primary_key=True)
    totalValue: Optional[float] = Column(Float, nullable=True)


class BourseviewCompaniesTransactionsRaw(Base):
    __tablename__ = "companies_transactions_raw"
    __table_args__ = {"schema": "bourseview"}

    shareCount: Optional[float] = Column(Float, nullable=True)
    sharePrice: Optional[float] = Column(Float, nullable=True)
    inTotalCost: Optional[float] = Column(Float, nullable=True)
    benefitLoss: Optional[float] = Column(Float, nullable=True)
    companySymbol: Optional[str] = Column(Text, nullable=True)
    companyName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    outTotalCost: Optional[float] = Column(Float, nullable=True)
    shareValue: Optional[float] = Column(Float, nullable=True)
    totalCost: Optional[float] = Column(Float, nullable=True)
    totalValue: Optional[float] = Column(Float, nullable=True)
    type: Optional[float] = Column(Float, nullable=False, primary_key=True)
    isin: Optional[str] = Column(Text, nullable=True)
    company: Optional[str] = Column(Text, nullable=False, primary_key=True)
    month: Optional[str] = Column(Text, nullable=False, primary_key=True)


class BourseviewCiq(Base):
    __tablename__ = "ciq"
    __table_args__ = {"schema": "bourseview"}

    periodEndingDate: Optional[int] = Column(BigInteger, nullable=False, primary_key=True)
    fiscalMonth: Optional[int] = Column(BigInteger, nullable=True)
    stock: Optional[str] = Column(Text, nullable=False, primary_key=True)
    industry: Optional[str] = Column(Text, nullable=True)
    statementItemName: Optional[str] = Column(Text, nullable=False, primary_key=True)
    valueOrigin: Optional[str] = Column(Text, nullable=True)
    valueVertical: Optional[str] = Column(Text, nullable=True)
    jdate: Optional[str] = Column(Text, nullable=True)
    jyear: Optional[str] = Column(Text, nullable=True)
    jdate_int: Optional[int] = Column(BigInteger, nullable=True)
    valueOrigin_lag1: Optional[str] = Column(Text, nullable=True)
    valueOrigin_lag_growth: Optional[str] = Column(Text, nullable=True)
    valueOrigin_lag_growth_per: Optional[str] = Column(Text, nullable=True)
    valueOrigin_lag4: Optional[str] = Column(Text, nullable=True)
    valueOrigin_ma4: Optional[str] = Column(Text, nullable=True)
    valueVertical_lag1: Optional[str] = Column(Text, nullable=True)
    valueVertical_lag_growth: Optional[str] = Column(Text, nullable=True)
    valueVertical_lag_growth_per: Optional[str] = Column(Text, nullable=True)
    valueVertical_lag4: Optional[str] = Column(Text, nullable=True)
    valueVertical_ma4: Optional[str] = Column(Text, nullable=True)
    quarter: Optional[int] = Column(BigInteger, nullable=True)

