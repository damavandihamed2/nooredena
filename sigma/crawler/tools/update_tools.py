import jdatetime
import pandas as pd
from pyodbc import Connection


one_day = jdatetime.timedelta(days=1)

cols_mapper_buysell = {
    "Asset": "asset", "TradeSymbol": "symbol", "PaymentDisplay": "broker", "Date": "date", 'ActionType': "action",
    "AssetType": "type", "Exchange": "market", 'Category': "sector", "Count": "amount", 'Price': "price",
    'Amount': "value", "LiabilityAmount": "gross_financial_liability", 'BrokerCommission': "broker_cost",
    'Commission': "cost", 'SEOCost': "organization_cost", 'Tax': "tax_cost", 'PureProfitLoss': "net_profit",
    'BrokerCommissionDiscount': "broker_discount", 'Discount': "total_discount", 'OtherCosts': "other_cost",
    'DiscountCost': "organization_discount", 'TaxDiscount': "tax_discount", 'PurePrice': "net_price",
    'TotalAmount': "net_value", "LiabilityTotalAmount": "net_financial_liability", 'TotalCost': "total_cost",
    'ProfitLoss': "gross_profit", "FiscalYear": "basket", "Interest": "interest",
    "PeriodValuation": "period_valuation"}
cols_mapper_profitloss = {
    'Asset': "asset", 'PortfolioName': "basket", 'PaymentDisplay': "broker", 'Interest': "interest", 'Date': "date",
    'AssetType': "type", 'Count': "amount", 'Price': "price", 'Amount': "value", 'BrokerCommission': "broker_cost",
    'SEOCost': "organization_cost", 'Tax': "tax_cost", 'Commission': "cost", 'PeriodValuation': "period_valuation",
    'BrokerCommissionDiscount': "broker_discount", 'DiscountCost': "organization_discount", 'PurePrice': "net_price",
    'TaxDiscount': "tax_discount", 'Discount': "total_discount", 'OtherCosts': "other_cost", 'TotalAmount': "net_value",
    'TotalCost': "total_cost", 'ExerciseAmount': "option_exercise_cost", 'PureProfitLossPercent': "net_profit_percent",
    'PureProfitLoss': "net_profit", 'ProfitLoss': "gross_profit", 'ProfitLossPercent': "gross_profit_percent"}
cols_mapper_portfolio = {
    "date": "date", 'pure_gain_percentage_with_close_price': 'net_profit_final_price_percent',
    'break_even_price': 'break_even_point', 'supervising_broker_display': 'supervising_broker', 'asset_display': 'asset',
    'value_with_close_price': 'gross_value_final_price', 'total_count': 'amount', 'total_contract_count': 'put_option',
    'value_with_close_price_to_sum': 'gross_value_final_price_ratio', 'sell_commission_with_close_price': 'commission',
    'gain_with_close_price': 'gross_profit_final_price', 'category_display': 'sector', 'asset_type_display': 'type',
    'gain_percentage_with_close_price': 'gross_profit_final_price_percent', 'asset_trade_symbol_display': 'symbol',
    'pure_value_with_real_close_price': 'net_value_final_price', 'total_cost_ratio_to_sum': 'total_cost_ratio',
    'pure_gain_with_close_price': 'net_profit_final_price', 'average_cost': 'cost_per_share', 'portfolio_name': 'basket',
    'pure_value_with_close_price_to_sum': 'net_value_final_price_ratio', 'real_close_price': 'last_price',
    'value_with_real_close_price': 'gross_value_last_price', 'exchange_display': 'market', 'close_price': 'final_price',
    'value_with_real_close_price_to_sum': 'gross_value_last_price_ratio', 'sell_discount_with_close_price': 'discount',
    'gain_with_real_close_price': 'gross_profit_last_price', 'total_liability': 'financial_liability', 'state': 'status',
    'gain_percentage_with_real_close_price': 'gross_profit_last_price_percent', 'company_capital': 'end_period_capital',
    'profit_loss': 'period_gross_profit_final_price', 'total_liability_ratio_to_sum': 'financial_liability_ratio',
    'ownership_percent': 'ownership_percentage', 'realized_eps': 'achieved_eps', 'fiscal_period': 'period', 'p2e': 'pe',
    'close_price_change_percentage': 'final_price_change', 'real_close_price_change_percentage': 'last_price_change'}
cols_mapper_dividend = {
    'Company': 'asset', 'FiscalYearDate': 'fiscal_year', 'Date': 'meeting_date', 'ExercisedWarrant': 'payment_value',
    'PaymentDate': 'payment_date', 'AwardedAmount': 'value', 'Description': 'status', # 'FiscalYear': 'basket',
    'EventDate': 'date', 'RealizedEps': 'eps', 'Dps': 'dps', 'Count': 'amount', 'ReceivedAmount': 'value_received',
    'CorrectionAmount': 'value_correction'}


dtype_mapper_buysell = {
    "broker_cost": "int64", "organization_cost": "int64", "tax_cost": "int64", "broker_discount": "int64",
    "organization_discount": "int64", "tax_discount": "int64", "total_discount": "int64", "net_value": "int64",
    "total_cost": "int64", "net_profit": "int64", "gross_profit": "int64"
}

def get_last_buysell_date(portfolio_id: int, db_connection: Connection) -> str:
    last_buysell = pd.read_sql(
        f"select max(date) date FROM [nooredenadb].[sigma].[buysell] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return last_buysell

def get_last_profitloss_date(portfolio_id: int, db_connection: Connection) -> str:
    last_profitloss = pd.read_sql(
        f"select max(date) date FROM [nooredenadb].[sigma].[profitloss] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return last_profitloss

def get_last_dividend_date(portfolio_id: int, db_connection: Connection) -> str:
    last_dividend = pd.read_sql(
        f"SELECT MAX(meeting_date) date FROM [nooredenadb].[sigma].[dividend] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return last_dividend

def get_last_portfolio_date(portfolio_id: int, db_connection: Connection) -> str:
    last_portfolio = pd.read_sql(
        f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[portfolio] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return last_portfolio

def get_end_portfolio_date(portfolio_id: int, db_connection: Connection) -> str:
    end_portfolio = pd.read_sql(
        f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[buysell] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return end_portfolio

def get_end_dividend_date(portfolio_id: int, db_connection: Connection) -> str:
    end_dividend = pd.read_sql(
        f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[portfolio] WHERE portfolio_id={portfolio_id}",
        db_connection
    )["date"].iloc[0]
    return end_dividend




def get_start_date(last_date: str) -> str:
    if last_date:
        start_date = jdatetime.datetime.strptime(last_date, format="%Y/%m/%d")
        start_date = start_date + one_day
        start_date = start_date.strftime("%Y/%m/%d")
    else:
        start_date = "1400/01/01"
    return start_date



