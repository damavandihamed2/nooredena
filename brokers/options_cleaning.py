import warnings
import pandas as pd
import datetime, jdatetime

from utils.database import make_connection, insert_to_database


warnings.filterwarnings('ignore')
db_conn = make_connection()


def g_t_j(d: str) -> str:
    d = datetime.datetime.strptime(d, "%Y-%m-%d")
    d = jdatetime.datetime.fromgregorian(
        year=d.year, month=d.month, day=d.day).strftime("%Y/%m/%d")
    return d

####################################################################################################

omex = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[option_settlements_omex] WHERE status!='Failed'", db_conn)
omex.drop(columns=["id", "instrumentId", "rValue", "rMaximum", "rFraction", "csDate", "status", "statusName", 'rlp',
                   'physicalSettlementValue', 'cashSettlementValue', "openPositionCount", "psDate", "createdDate",
                   "requestDate", "resultDate", "lVal30", 'autoValue', 'peValue', "ceValue", "ceaValue"], inplace=True)
omex.rename({"side": "type", "instrumentName": "symbol", "cSize": "contract_size", "strikePrice": "strike_price",
             "eDate": "date", "settlementType": "exercise_type"}, axis=1, inplace=True)
omex["type"].replace({"Buy": 1, "Sell": 2}, inplace=True)
cash = omex[omex["exercise_type"] == "Cash"]
cash = cash.drop(['penValue', 'penAmount'], axis=1, inplace=False)
cash.rename({"eValue": "volume", "peAmount": "value"}, axis=1, inplace=True)
cash = cash[cash["volume"] > 0]
physical = omex[omex["exercise_type"] == "Physical"]
physical_default = physical[physical["penValue"] > 0]
physical = physical[physical["eValue"] > 0]
physical.rename({"eValue": "volume", "peAmount": "value"}, axis=1, inplace=True)
physical.drop(["penValue", "penAmount"], axis=1, inplace=True)
physical_default.drop(["eValue", "peAmount"], axis=1, inplace=True)
physical_default.rename({"penValue": "volume", "penAmount": "value"}, axis=1, inplace=True)
cash["exercise_type"] = 1
physical["exercise_type"] = 2
physical_default["exercise_type"] = 3
omex = pd.concat([cash, physical, physical_default], axis=0, ignore_index=True)
omex["date"] = omex["date"].str[:10].apply(g_t_j)

online_plus = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[option_settlements_online_plus]", db_conn)
cash = online_plus[['Symbol', 'SettlementDate', 'StrikePrice', 'CSize', 'CashValue', 'CEValue',
                    'CashBuyOP', 'CashSellOP', 'CashSettlementValue', 'portfolio_id', 'broker_id']]
cash = cash[(cash['CashBuyOP'] > 0) | (cash['CashSellOP'] > 0)]
cash["type"] = (cash['CashBuyOP'] > 0) * 1 + (cash['CashSellOP'] > 0) * 2
cash.drop(columns=['CashValue', 'CashBuyOP', 'CashSellOP'], inplace=True)
cash.rename({"CEValue": "volume", "CashSettlementValue": "value"}, axis=1, inplace=True)
physical = online_plus[['Symbol', 'SettlementDate', 'StrikePrice', 'CSize', 'PhyscValue',
                        'PEValue', 'PhysicalBuyOP', 'PhysicalSellOP', 'PhysicalSettlementValue',
                        'PENValue', 'DefaultValue', 'portfolio_id', 'broker_id']]
physical = physical[(physical['PhysicalBuyOP'] > 0) | (physical['PhysicalSellOP'] > 0)]
physical["type"] = (physical['PhysicalBuyOP'] > 0) * 1 + (physical['PhysicalSellOP'] > 0) * 2
physical.drop(columns=['PhyscValue', 'PhysicalBuyOP', 'PhysicalSellOP'], inplace=True)
physical.rename({"PEValue": "volume", "PhysicalSettlementValue": "value"}, axis=1, inplace=True)
physical_default = physical[['Symbol', 'SettlementDate', 'StrikePrice', 'CSize', 'PENValue',
                             'DefaultValue', 'type', 'portfolio_id', 'broker_id']]
physical_default.rename({'PENValue': "volume", 'DefaultValue': "value"}, axis=1, inplace=True)
physical.drop(columns=['PENValue', 'DefaultValue'], inplace=True)
cash["exercise_type"] = 1
physical["exercise_type"] = 2
physical_default["exercise_type"] = 3
online_plus = pd.concat([physical, physical_default, cash], axis=0, ignore_index=True)
online_plus = online_plus[online_plus["volume"] > 0]
online_plus.rename({"Symbol": "symbol", "SettlementDate": "date", "StrikePrice": "strike_price",
                    "CSize": "contract_size"}, axis=1, inplace=True)
online_plus["date"] = online_plus["date"].str.replace("-", "/")

options = pd.concat([omex, online_plus], axis=0, ignore_index=True)
options.sort_values(by="date", ignore_index=True, inplace=True)
options["symbol"].replace({"ی": "ي", "ک": "ك"}, regex=True, inplace=True)
options["volume"] = options["volume"].abs().astype(int)
options["value"] = options["value"].abs().astype(int)
options["option_type"] = ((options["symbol"].str[:1] == "ض") * 1 +
                          (options["symbol"].str[:1] == "ط") * 2 + (options["symbol"].str[:1] == "ه") * 3)
last_date = pd.read_sql("SELECT MAX(date) FROM [nooredenadb].[brokers].[option_settlements]", db_conn)[""].iloc[0]
crsr = db_conn.cursor()
crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[option_settlements] WHERE date >= '{last_date}'")
crsr.close()
options = options[options["date"] >= last_date]
insert_to_database(dataframe=options, database_table="[nooredenadb].[brokers].[option_settlements]")

####################################################################################################

omex_portfolio = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[option_portfolio_omex]", db_conn)
omex_portfolio.drop(labels=["principalId", "lVal30", "baseInstrumentId", "csDate", "psDate", "remainCsDateDays",
                            "versionId","totalSellRevenue", "blockedStrategyQuantity", "remainingStrategyQuantity",
                            "initialMargin", "requiredMargin", "maintenanceMargin", "maxCOP", "strikePrice",
                            "optionSide"], axis=1, inplace=True)
omex_portfolio.rename({"instrumentName": "symbol", "instrumentId": "symbol_code", "orderSide": "type", "strikePrice": "strike_price",
                       "count": "volume", "cSize": "contract_size", "eDate": "due_date", "executedPrice": "mean_price",
                       "breakEvenPrice": "break_even_price", "blockedAmount": "margin_value"}, axis=1, inplace=True)
omex_portfolio["type"] = (omex_portfolio["type"] == "Buy") * 1 + (omex_portfolio["type"] == "Sell") * 2

online_plus_portfolio = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[option_portfolio_online_plus]", db_conn)
online_plus_portfolio.drop(labels=["CustomerTitle", "PSDate", "MarketType", "SymbolId", "CustomerId", "SymbolNoteText",
                                   "ColaturalCount", "CurrencyPrice", "MarketUnitName", "SellOrderCount",
                                   "BuyOrderCount", "SellTotalCount", "BuyTotalCount", "MarginCount",
                                   "BlockedMarginCount"], axis=1, inplace=True)
online_plus_portfolio.rename({"Symbol": "symbol", "NSCCode": "symbol_code", "PSDateDate": "due_date",
                              "CSize": "contract_size","SellPositionCount": "volume_sell", "StrikePrice": "strike_price",
                              "BuyPositionCount": "volume_buy", "MarginValue": "margin_value"}, axis=1, inplace=True)
online_plus_portfolio["type"] = (online_plus_portfolio["volume_buy"] > 0) * 1 + (online_plus_portfolio["volume_sell"] > 0) * 2
online_plus_portfolio["volume"] = online_plus_portfolio["volume_buy"] + online_plus_portfolio["volume_sell"]
online_plus_portfolio.drop(labels=["volume_buy", "volume_sell"], axis=1, inplace=True)
online_plus_portfolio["symbol"] = online_plus_portfolio["symbol"].str[:-1]

options_portfolio = pd.concat([omex_portfolio, online_plus_portfolio], axis=0, ignore_index=True)
options_portfolio["symbol"].replace({"ی": "ي", "ک": "ك"}, regex=True, inplace=True)
online_plus_portfolio["volume"] = online_plus_portfolio["volume"].abs()
options_portfolio["option_type"] = ((options_portfolio["symbol"].str[:1] == "ض") * 1 +
                                    (options_portfolio["symbol"].str[:1] == "ط") * 2 +
                                    (options_portfolio["symbol"].str[:1] == "ه") * 3)
options_portfolio["due_date"] = options_portfolio["due_date"].str[:10].apply(g_t_j)

crsr = db_conn.cursor()
crsr.execute(f"TRUNCATE TABLE [nooredenadb].[brokers].[option_portfolio]")
crsr.close()
insert_to_database(dataframe=options_portfolio, database_table="[nooredenadb].[brokers].[option_portfolio]")
