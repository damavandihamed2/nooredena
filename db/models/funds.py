from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, BigInteger, Float, Integer, SmallInteger, String, Text
from .base import Base


class FundsFixes(Base):
    __tablename__ = "fixes"
    __table_args__ = {"schema": "funds"}

    regNo: Optional[str] = Column(Text, nullable=False, primary_key=True)
    fundType: Optional[str] = Column(Text, nullable=True)
    netAsset: Optional[float] = Column(Float, nullable=True)
    typeOfInvest: Optional[str] = Column(Text, nullable=True)
    dailyEfficiency: Optional[float] = Column(Float, nullable=True)
    weeklyEfficiency: Optional[float] = Column(Float, nullable=True)
    monthlyEfficiency: Optional[float] = Column(Float, nullable=True)
    investedUnits: Optional[float] = Column(Float, nullable=True)
    quarterlyEfficiency: Optional[float] = Column(Float, nullable=True)
    sixMonthEfficiency: Optional[float] = Column(Float, nullable=True)
    annualEfficiency: Optional[float] = Column(Float, nullable=True)
    statisticalNav: Optional[float] = Column(Float, nullable=True)
    cancelNav: Optional[float] = Column(Float, nullable=True)
    issueNav: Optional[float] = Column(Float, nullable=True)
    manager: Optional[str] = Column(Text, nullable=True)
    smallSymbolName: Optional[str] = Column(Text, nullable=True)
    stock: Optional[float] = Column(Float, nullable=True)
    date: Optional[str] = Column(Text, nullable=True)
    fi_save: Optional[float] = Column(Float, nullable=True)
    fi_save_percent: Optional[float] = Column(Float, nullable=True)
    dailyEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    weeklyEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    monthlyEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    quarterlyEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    sixMonthEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    annualEfficiencyRank: Optional[float] = Column(Float, nullable=True)
    ranksSum: Optional[float] = Column(Float, nullable=True)


class FundsFundsDetailData(Base):
    __tablename__ = "funds_detail_data"
    __table_args__ = {"schema": "funds"}

    regNo: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    name: Optional[str] = Column(String(255), nullable=True)
    initiationDate: Optional[str] = Column(String(50), nullable=True)
    fundSize: Optional[int] = Column(BigInteger, nullable=True)
    fundType: Optional[int] = Column(Integer, nullable=True)
    executiveManager: Optional[str] = Column(Text, nullable=True)
    lastModificationTime: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column(String(50), nullable=True)
    dailyEfficiency: Optional[float] = Column(Float, nullable=True)
    weeklyEfficiency: Optional[float] = Column(Float, nullable=True)
    monthlyEfficiency: Optional[float] = Column(Float, nullable=True)
    quarterlyEfficiency: Optional[float] = Column(Float, nullable=True)
    sixMonthEfficiency: Optional[float] = Column(Float, nullable=True)
    annualEfficiency: Optional[float] = Column(Float, nullable=True)
    dividendIntervalPeriod: Optional[int] = Column(Integer, nullable=True)
    estimatedEarningRate: Optional[float] = Column(Float, nullable=True)
    guaranteedEarningRate: Optional[float] = Column(Float, nullable=True)
    insInvNo: Optional[int] = Column(Integer, nullable=True)
    insInvPercent: Optional[float] = Column(Float, nullable=True)
    legalPercent: Optional[float] = Column(Float, nullable=True)
    marketMaker: Optional[str] = Column(String(255), nullable=True)
    naturalPercent: Optional[float] = Column(Float, nullable=True)
    netAsset: Optional[int] = Column(BigInteger, nullable=True)
    retInvNo: Optional[int] = Column(Integer, nullable=True)
    retInvPercent: Optional[float] = Column(Float, nullable=True)
    investedUnits: Optional[int] = Column(BigInteger, nullable=True)
    unitsRedDAY: Optional[int] = Column(BigInteger, nullable=True)
    unitsRedFromFirst: Optional[int] = Column(BigInteger, nullable=True)
    unitsSubDAY: Optional[int] = Column(BigInteger, nullable=True)
    unitsSubFromFirst: Optional[int] = Column(BigInteger, nullable=True)
    efficiency: Optional[float] = Column(Float, nullable=True)
    cancelNav: Optional[int] = Column(Integer, nullable=True)
    issueNav: Optional[int] = Column(Integer, nullable=True)
    statisticalNav: Optional[int] = Column(Integer, nullable=True)
    fiveBest: Optional[float] = Column(Float, nullable=True)
    stock: Optional[float] = Column(Float, nullable=True)
    bond: Optional[float] = Column(Float, nullable=True)
    other: Optional[float] = Column(Float, nullable=True)
    cash: Optional[float] = Column(Float, nullable=True)
    deposit: Optional[float] = Column(Float, nullable=True)
    fundUnit: Optional[float] = Column(Float, nullable=True)
    commodity: Optional[float] = Column(Float, nullable=True)
    ETF: Optional[int] = Column(SmallInteger, nullable=True)
    rankLastUpdate: Optional[str] = Column(String(50), nullable=True)
    manager: Optional[str] = Column(String(255), nullable=True)
    managerSeoRegisterNo: Optional[str] = Column(String(50), nullable=True)
    guarantorSeoRegisterNo: Optional[str] = Column(String(50), nullable=True)
    auditor: Optional[str] = Column(String(255), nullable=True)
    websiteAddress: Optional[str] = Column(String(255), nullable=True)
    custodian: Optional[str] = Column(String(255), nullable=True)
    guarantor: Optional[str] = Column(String(255), nullable=True)
    investmentManager: Optional[str] = Column(String(255), nullable=True)
    beta: Optional[float] = Column(Float, nullable=True)
    alpha: Optional[float] = Column(Float, nullable=True)
    seoRegisterDate: Optional[str] = Column(String(50), nullable=True)
    registrationNumber: Optional[str] = Column(String(50), nullable=True)
    registerDate: Optional[str] = Column(String(50), nullable=True)
    nationalId: Optional[str] = Column(String(50), nullable=True)
    isCompleted: Optional[int] = Column(SmallInteger, nullable=True)
    insCode: Optional[str] = Column(String(50), nullable=True)
    baseUnitsSubscriptionNAV: Optional[float] = Column(Float, nullable=True)
    baseUnitsCancelNAV: Optional[float] = Column(Float, nullable=True)
    baseUnitsTotalNetAssetValue: Optional[int] = Column(BigInteger, nullable=True)
    baseTotalUnit: Optional[int] = Column(BigInteger, nullable=True)
    baseUnitsTotalSubscription: Optional[float] = Column(Float, nullable=True)
    baseUnitsTotalCancel: Optional[float] = Column(Float, nullable=True)
    superUnitsSubscriptionNav: Optional[float] = Column(Float, nullable=True)
    superUnitsCancelNAV: Optional[float] = Column(Float, nullable=True)
    superUnitsTotalNetAssetValue: Optional[int] = Column(BigInteger, nullable=True)
    superTotalUnit: Optional[int] = Column(BigInteger, nullable=True)
    superUnitsTotalSubscription: Optional[float] = Column(Float, nullable=True)
    superUnitsTotalCancel: Optional[float] = Column(Float, nullable=True)
    fundPublisher: Optional[int] = Column(Integer, nullable=True)
    smallSymbolName: Optional[str] = Column(String(50), nullable=True)
    date: Optional[str] = Column("date_", String(50), nullable=True)
    time: Optional[str] = Column("time_", String(50), nullable=True)


class FundsFundsInvTrades(Base):
    __tablename__ = "funds_inv_trades"
    __table_args__ = {"schema": "funds"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company_type: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    action: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    value: Optional[int] = Column(BigInteger, nullable=True)


class FundsFundsInvTradesRaw(Base):
    __tablename__ = "funds_inv_trades_raw"
    __table_args__ = {"schema": "funds"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    company_type: Optional[int] = Column(SmallInteger, nullable=False, primary_key=True)
    symbol_name: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    buy_value: Optional[int] = Column(BigInteger, nullable=True)
    sell_value: Optional[int] = Column(BigInteger, nullable=True)


class FundsFundsLevProspectus(Base):
    __tablename__ = "funds_lev_Prospectus"
    __table_args__ = {"schema": "funds"}

    jdate: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    max_lev_units: Optional[int] = Column(BigInteger, nullable=True)
    min_rate: Optional[float] = Column(Float, nullable=True)
    max_rate: Optional[float] = Column(Float, nullable=True)


class FundsFundsLevNav(Base):
    __tablename__ = "funds_lev_nav"
    __table_args__ = {"schema": "funds"}

    jdate: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    lev_issue_price: Optional[int] = Column(Integer, nullable=True)
    lev_cancel_price: Optional[int] = Column(Integer, nullable=True)
    lev_units: Optional[int] = Column(BigInteger, nullable=True)
    lev_nav: Optional[int] = Column(BigInteger, nullable=True)
    fix_price: Optional[float] = Column(Float, nullable=True)
    fix_units: Optional[int] = Column(BigInteger, nullable=True)
    fix_nav: Optional[int] = Column(BigInteger, nullable=True)
    fix_individuals: Optional[int] = Column(Integer, nullable=True)
    units: Optional[int] = Column(BigInteger, nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    lev_ratio: Optional[float] = Column(Float, nullable=True)
    lev_issue_units: Optional[int] = Column(BigInteger, nullable=True)
    lev_issues: Optional[int] = Column(BigInteger, nullable=True)
    lev_cancel_units: Optional[int] = Column(BigInteger, nullable=True)
    lev_cancels: Optional[int] = Column(BigInteger, nullable=True)
    lev_net_issues: Optional[int] = Column(BigInteger, nullable=True)
    fix_issue_units: Optional[int] = Column(BigInteger, nullable=True)
    fix_issues: Optional[int] = Column(BigInteger, nullable=True)
    fix_cancel_units: Optional[int] = Column(BigInteger, nullable=True)
    fix_cancels: Optional[int] = Column(BigInteger, nullable=True)
    fix_net_issues: Optional[int] = Column(BigInteger, nullable=True)


class FundsFundsLevPortfolio(Base):
    __tablename__ = "funds_lev_portfolio"
    __table_args__ = {"schema": "funds"}

    jdate: Optional[int] = Column(Integer, nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    symbol_id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    trade_value: Optional[int] = Column(BigInteger, nullable=True)
    top5: Optional[int] = Column(BigInteger, nullable=True)
    top5_percent: Optional[float] = Column(Float, nullable=True)
    other_stocks: Optional[int] = Column(BigInteger, nullable=True)
    other_stocks_percent: Optional[float] = Column(Float, nullable=True)
    stocks: Optional[int] = Column(BigInteger, nullable=True)
    stocks_percent: Optional[float] = Column(Float, nullable=True)
    bond: Optional[int] = Column(BigInteger, nullable=True)
    bond_percent: Optional[float] = Column(Float, nullable=True)
    bill: Optional[int] = Column(BigInteger, nullable=True)
    bill_percent: Optional[float] = Column(Float, nullable=True)
    fix: Optional[int] = Column(BigInteger, nullable=True)
    fix_percent: Optional[float] = Column(Float, nullable=True)
    cash: Optional[int] = Column(BigInteger, nullable=True)
    cash_percent: Optional[float] = Column(Float, nullable=True)
    others: Optional[int] = Column(BigInteger, nullable=True)
    others_percent: Optional[float] = Column(Float, nullable=True)
    fund_units: Optional[int] = Column(BigInteger, nullable=True)
    fund_units_percent: Optional[float] = Column(Float, nullable=True)


class FundsFundsPortfolio(Base):
    __tablename__ = "funds_portfolio"
    __table_args__ = {"schema": "funds"}

    date: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    fund: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    value_in_portfolio: Optional[int] = Column(BigInteger, nullable=True)


class FundsFundsSymbolMapper(Base):
    __tablename__ = "funds_symbol_mapper"
    __table_args__ = {"schema": "funds"}

    symbol_name: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    symbol: Optional[str] = Column(String(50), nullable=False, primary_key=True)


class FundsFundsTransactions(Base):
    __tablename__ = "funds_transactions"
    __table_args__ = {"schema": "funds"}

    month: Optional[str] = Column(Text, nullable=False, primary_key=True)
    fund: Optional[str] = Column(Text, nullable=False, primary_key=True)
    per_total: Optional[float] = Column(Float, nullable=True)
    symbol: Optional[str] = Column(Text, nullable=False, primary_key=True)
    industry: Optional[str] = Column(Text, nullable=True)
    type: Optional[str] = Column(Text, nullable=False, primary_key=True)
    totalValue: Optional[float] = Column(Float, nullable=True)


class FundsFundsTransactionsRaw(Base):
    __tablename__ = "funds_transactions_raw"
    __table_args__ = {"schema": "funds"}

    year: Optional[int] = Column(BigInteger, nullable=True)
    month: Optional[str] = Column(Text, nullable=False, primary_key=True)
    fund: Optional[str] = Column(Text, nullable=False, primary_key=True)
    name: Optional[str] = Column(Text, nullable=True)
    buy: Optional[float] = Column(Float, nullable=True)
    sell: Optional[float] = Column(Float, nullable=True)
    value: Optional[float] = Column(Float, nullable=True)
    per_total: Optional[float] = Column(Float, nullable=True)
    symbol: Optional[str] = Column(Text, nullable=False, primary_key=True)
    industry: Optional[str] = Column(Text, nullable=True)


class FundsGoldData(Base):
    __tablename__ = "gold_data"
    __table_args__ = {"schema": "funds"}

    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)
    jdate: Optional[str] = Column(String(50), nullable=True)
    issue_price: Optional[float] = Column(Float, nullable=True)
    cancel_price: Optional[float] = Column(Float, nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    fund: Optional[str] = Column(String(50), nullable=True)
    id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    manager: Optional[str] = Column(String(255), nullable=True)
    name: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    website: Optional[str] = Column(String(255), nullable=True)
    source: Optional[str] = Column(String(50), nullable=True)
    final_price: Optional[int] = Column(Integer, nullable=True)
    daily_return: Optional[float] = Column(Float, nullable=True)
    weekly_return: Optional[float] = Column(Float, nullable=True)
    monthly_return: Optional[float] = Column(Float, nullable=True)
    annual_return: Optional[float] = Column(Float, nullable=True)
    PNAV: Optional[float] = Column(Float, nullable=True)
    pbubble: Optional[float] = Column(Float, nullable=True)
    ibubble: Optional[float] = Column(Float, nullable=True)
    gbubble: Optional[float] = Column(Float, nullable=True)
    gold: Optional[float] = Column(Float, nullable=True)
    coin: Optional[float] = Column(Float, nullable=True)
    other: Optional[float] = Column(Float, nullable=True)
    dollar: Optional[float] = Column(Float, nullable=True)
    isin: Optional[str] = Column(String(50), nullable=True)
    last_update: Optional[int] = Column(Integer, nullable=True)
    buy_vol_i: Optional[int] = Column(BigInteger, nullable=True)
    buy_no_i: Optional[int] = Column(Integer, nullable=True)
    sell_vol_i: Optional[int] = Column(BigInteger, nullable=True)
    sell_no_i: Optional[int] = Column(Integer, nullable=True)
    buy_vol_c: Optional[int] = Column(BigInteger, nullable=True)
    buy_no_c: Optional[int] = Column(Integer, nullable=True)
    sell_vol_c: Optional[int] = Column(BigInteger, nullable=True)
    sell_no_c: Optional[int] = Column(Integer, nullable=True)
    net_i: Optional[int] = Column(BigInteger, nullable=True)
    net_c: Optional[int] = Column(BigInteger, nullable=True)


class FundsGoldMetals(Base):
    __tablename__ = "gold_metals"
    __table_args__ = {"schema": "funds"}

    date: Optional[str] = Column(String(50), nullable=True)
    time: Optional[str] = Column(String(50), nullable=True)
    symbol: Optional[str] = Column(String(255), nullable=False, primary_key=True)
    final_price: Optional[int] = Column(BigInteger, nullable=True)
    iv: Optional[float] = Column(Float, nullable=True)
    bubble: Optional[float] = Column(Float, nullable=True)
    pbubble: Optional[float] = Column(Float, nullable=True)
    dollar: Optional[float] = Column(Float, nullable=True)


class FundsGoldSpecification(Base):
    __tablename__ = "gold_specification"
    __table_args__ = {"schema": "funds"}

    id: Optional[str] = Column(String(50), nullable=False, primary_key=True)
    fund: Optional[str] = Column(String(50), nullable=True)
    manager: Optional[str] = Column(String(255), nullable=True)
    name: Optional[str] = Column(String(255), nullable=True)
    symbol: Optional[str] = Column(String(50), nullable=True)
    website: Optional[str] = Column(String(255), nullable=True)
    source: Optional[str] = Column(String(50), nullable=True)
    isin: Optional[str] = Column(String(50), nullable=True)
    nav: Optional[int] = Column(BigInteger, nullable=True)
    last_update: Optional[int] = Column(Integer, nullable=True)
