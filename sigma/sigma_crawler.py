import pandas as pd
import warnings, jdatetime

from sigma import sigma
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
powerbi_database = make_connection()
authenticator = [{"username": "damavandi", "password": "Dd@123456"}]
today_ = jdatetime.datetime.now().strftime(format="%Y/%m/%d")
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
    "date": "date", 'Exchange': 'market', 'Type': 'type', 'State': 'status', 'SuperVisorBroker': 'supervising_broker',
    'TotalContractCount': 'put_option', 'MatureDate': 'due_date', 'BreakEvenPrice': 'break_even_point',
    'ClosePrice': 'final_price', 'RealClosePrice': 'last_price', 'OwnershipPercent': 'ownership_percentage',
    'NetForecastEps': 'forecast_eps', 'RealizedEps': 'achieved_eps', 'FiscalPeriod': 'period', 'P2E': 'pe',
    'FiscalYear': 'fiscal_year', 'OptionDueAmount': 'financial_liability', 'SellDiscountWithClosePrice': 'discount',
    'SellCommissionWithClosePrice': 'commission', 'InterestRate': 'interest_rate', 'AverageCost': 'cost_per_share',
    'ValueWithRealClosePrice': 'gross_value_last_price', 'PureValueWithClosePrice': 'net_value_final_price',
    'OptionDueAmountRatioToSum': 'financial_liability_ratio', 'ValueWithClosePrice': 'gross_value_final_price',
    'ValueWithClosePriceToSum': 'gross_value_final_price_ratio', 'GainWithClosePrice': 'gross_profit_final_price',
    'GainPercentageWithClosePrice': 'gross_profit_final_price_percent', 'CompanyCapital': 'end_period_capital',
    'PureGainPercentageWithClosePrice': 'net_profit_final_price_percent', 'TotalCost': 'total_cost',
    'PureValueWithClosePriceToSum': 'net_value_final_price_ratio', 'TotalCostRatioToSum': 'total_cost_ratio',
    'PureGainWithClosePrice': 'net_profit_final_price', 'GainWithRealClosePrice': 'gross_profit_last_price',
    'ValueWithRealClosePriceToSum': 'gross_value_last_price_ratio', 'Profitloss': 'period_gross_profit_final_price',
    'GainPercentageWithRealClosePrice': 'gross_profit_last_price_percent', 'PortfolioName': 'basket',
    'TotalClosePriceValueChangeDiffPercent': 'final_price_change', 'TradeSymbol': 'symbol', 'Count': 'amount',
    'TotalRealClosePriceValueChangeDiffPercent': 'last_price_change', 'Category': 'sector', 'Asset': 'asset'}
cols_mapper_dividend = {
    'Company': 'asset', 'FiscalYearDate': 'fiscal_year', 'Date': 'meeting_date', 'ExercisedWarrant': 'payment_value',
    'PaymentDate': 'payment_date', 'AwardedAmount': 'value', 'Description': 'status', # 'FiscalYear': 'basket',
    'EventDate': 'date', 'RealizedEps': 'eps', 'Dps': 'dps', 'Count': 'amount', 'ReceivedAmount': 'value_received',
    'CorrectionAmount': 'value_correction'}


##############################
##### Login to Sigma
agent = sigma.Agent(username=authenticator[0]["username"], password=authenticator[0]["password"])
agent.login()


##############################
##### Updating BuySell Data
for pid in [1, 25]:
    portfoId = pid
    last_buysell = pd.read_sql(f"select max(date) date FROM [nooredenadb].[sigma].[buysell] "
                               f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]
    start_buysell = (jdatetime.datetime.strptime(last_buysell, format="%Y/%m/%d") + one_day).strftime("%Y/%m/%d")

    buysell_data = agent.get_buysell(start_date=start_buysell, end_date=today_, portfolio_id=portfoId)
    if len(buysell_data) > 0:
        buysell_df = pd.DataFrame(buysell_data)
        buysell_df = buysell_df[list(cols_mapper_buysell.keys())]
        buysell_df.rename(mapper=cols_mapper_buysell, axis=1, inplace=True)
        buysell_df = buysell_df.astype({
            "broker_cost": "int64", "organization_cost": "int64", "tax_cost": "int64", "broker_discount": "int64",
            "organization_discount": "int64", "tax_discount": "int64", "total_discount": "int64", "net_value": "int64",
            "total_cost": "int64", "net_profit": "int64", "gross_profit": "int64"})
        buysell_df["portfolio_id"] = portfoId
        insert_to_database(dataframe=buysell_df, database_table="[nooredenadb].[sigma].[buysell]")


##############################
##### Updating ProfitLoss Data
for pid in [1, 25]:
    portfoId = pid
    last_profitloss = pd.read_sql(f"select max(date) date FROM [nooredenadb].[sigma].[profitloss] "
                                  f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]
    if last_profitloss:
        start_profitloss = (jdatetime.datetime.strptime(last_profitloss, format="%Y/%m/%d") + one_day).strftime("%Y/%m/%d")
    else:
        start_profitloss = "1404/01/01"

    profitloss_data = agent.get_profitloss(start_date=start_profitloss, end_date=today_, portfolio_id=portfoId)
    if len(profitloss_data) > 0:
        profitloss_df = pd.DataFrame(profitloss_data)
        profitloss_df = profitloss_df[list(cols_mapper_profitloss.keys())]
        profitloss_df.rename(mapper=cols_mapper_profitloss, axis=1, inplace=True)
        profitloss_df["portfolio_id"] = portfoId
        insert_to_database(dataframe=profitloss_df, database_table="[nooredenadb].[sigma].[profitloss]")


##############################
##### Updating Portfolio Data
for pid in [1, 25]:
    portfoId = pid
    last_portfolio = pd.read_sql(f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[portfolio] "
                                 f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]
    if last_portfolio:
        start_portfolio = (jdatetime.datetime.strptime(last_portfolio, format="%Y/%m/%d") + one_day).strftime("%Y/%m/%d")
    else:
        start_portfolio = "1404/01/01"
    end_portfolio = pd.read_sql(f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[buysell] "
                                f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]

    if end_portfolio >= start_portfolio:
        portfolio = pd.DataFrame()
        date_ = start_portfolio
        while date_ <= end_portfolio:
            print(f"getting portfolio data for ({portfoId=} - {date_=})")
            portfolio_data = agent.get_portfolio(date=date_, portfolio_id=portfoId)
            if len(portfolio_data) > 0:
                portfolio_df = pd.DataFrame(portfolio_data)
                portfolio_df["date"] = date_
                portfolio = pd.concat([portfolio, portfolio_df], axis=0, ignore_index=True)
            date_ = (jdatetime.datetime.strptime(date_, format="%Y/%m/%d") + one_day).strftime(format="%Y/%m/%d")
        portfolio = portfolio[list(cols_mapper_portfolio.keys())]
        portfolio.rename(mapper=cols_mapper_portfolio, axis=1, inplace=True)
        portfolio["asset"] = portfolio["asset"].str.split("||", regex=False, expand=True)[0]
        portfolio["portfolio_id"] = portfoId
        insert_to_database(dataframe=portfolio, database_table="[nooredenadb].[sigma].[portfolio]")


##############################
##### Updating Dividend Data
for pid in [1, 25]:
    portfoId = pid
    lst_dividend = pd.read_sql(f"SELECT MAX(meeting_date) date FROM [nooredenadb].[sigma].[dividend] "
                               f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]
    if not lst_dividend:
        lst_dividend = "1404/01/01"
    end_dividend = pd.read_sql(f"SELECT MAX(date) date FROM [nooredenadb].[sigma].[portfolio] "
                               f"WHERE portfolio_id={portfoId}", powerbi_database)["date"].iloc[0]

    dividend_data = agent.get_dividend(start_date=lst_dividend, end_date=end_dividend, portfolio_id=portfoId)
    dividend_df = pd.DataFrame(dividend_data)
    dividend_df = dividend_df[list(cols_mapper_dividend.keys())]
    dividend_df.rename(mapper=cols_mapper_dividend, axis=1, inplace=True)
    dividend_df["portfolio_id"] = portfoId
    dividend_df = dividend_df[dividend_df["value"] > 0]
    dividend_df = dividend_df[dividend_df["meeting_date"] >= lst_dividend].reset_index(drop=True, inplace=False)

    if len(dividend_df) > 0:
        crsr = powerbi_database.cursor()
        crsr.execute(f"DELETE FROM [nooredenadb].[sigma].[dividend] "
                     f"WHERE portfolio_id={portfoId} AND meeting_date >= '{lst_dividend}'")
        crsr.close()
        insert_to_database(dataframe=dividend_df, database_table="[nooredenadb].[sigma].[dividend]")

