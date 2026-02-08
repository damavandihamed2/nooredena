import pandas as pd
from time import sleep
from pathlib import Path
import os, sys, random, warnings, jdatetime, subprocess, numpy

from brokers.utils.funcs import safe_login, safe_call_func
from brokers.tadbirpardaz.tadbirpardaz import BrokersTadbirpardaz
from brokers.rayanhamafza.rayanhamafza import BrokersRayanhamafza

from utils.custom_logger import get_logger
from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
powerbi_database = make_connection()

try:
    filename = os.path.basename(__file__)
except:
    filename = "brokers_trades.py"
logger = get_logger(db_connection=powerbi_database, file_name=filename,
                    project_dir=os.path.dirname(os.path.abspath(sys.argv[0])))

brokers_list = pd.read_sql("SELECT bc.broker_id,bc.portfolio_id,bc.username,bc.password,b.broker,b.type_offline,"
                           "b.address_offline FROM [nooredenadb].[brokers].[brokers_credential] bc LEFT JOIN "
                           "nooredenadb.brokers.brokers b ON bc.broker_id = b.broker_id WHERE bc.system_type=2 "
                           "AND bc.active=1", powerbi_database)
today = jdatetime.date.today().strftime("%Y/%m/%d")
thirty_days_ago = jdatetime.datetime.now() - jdatetime.timedelta(days=30)

##########################################################################

start_date_rayan = pd.read_sql("SELECT MAX(transactionDate) as date FROM [nooredenadb].[brokers].[trades_rayan]",
                               powerbi_database)["date"].iloc[0]
trades_all_raw = pd.DataFrame()
portfolio_df = pd.DataFrame()
for b in range(len(brokers_list)):
    if brokers_list["type_offline"].iloc[b] == "rayan":
        portfolioId = int(brokers_list["portfolio_id"].iloc[b])
        broker_name, brokerId = brokers_list["broker"].iloc[b], int(brokers_list["broker_id"].iloc[b])
        print(f'-- {broker_name} (PortfolioId: {portfolioId})')

        broker = safe_login(
            broker_class=BrokersRayanhamafza, url=brokers_list["address_offline"].iloc[b],
            username=brokers_list["username"].iloc[b], password=brokers_list["password"].iloc[b])
        safe_call_func(broker.get_assets)
        safe_call_func(broker.get_trades, asset_type="stock", from_date=start_date_rayan, to_date=today)
        safe_call_func(broker.get_purchase_upper_bound)

        purchase_upper_bound = broker.purchase_upper_bound["purchaseUpperBound"]
        remain, assets_value = broker.customer_last_remain, broker.assets_value

        crsr = powerbi_database.cursor()
        sql = ("UPDATE [nooredenadb].[brokers].[brokers_balance] SET purchase_upper_bound=?,remain=?,portfolio_value=? "
               "WHERE broker_id=? AND portfolio_id=?; IF @@ROWCOUNT=0 BEGIN INSERT INTO "
               "[nooredenadb].[brokers].[brokers_balance] (portfolio_id,broker_id,purchase_upper_bound,remain,"
               "portfolio_value,last_month_trades,credit) VALUES (?, ?, ?, ?, ?, ?, ?); END")
        params = (
            purchase_upper_bound, remain, assets_value, brokerId, portfolioId,
            portfolioId, brokerId, purchase_upper_bound, remain, assets_value, 0, 0
        )
        crsr.execute(sql, params)
        crsr.close()

        assets = pd.DataFrame(broker.assets)
        assets[["broker_id", "portfolio_id"]] = brokerId, portfolioId
        portfolio_df = pd.concat([portfolio_df, assets], axis=0, ignore_index=True)
        print(f'-- {broker_name} (PortfolioId: {portfolioId}) Broker Portfolio Done!')

        trades = pd.DataFrame(broker.trades)
        if len(trades) > 0:
            trades[["broker_id", "portfolio_id"]] = brokerId, portfolioId
            trades_all_raw = pd.concat([trades_all_raw, trades], axis=0, ignore_index=True)

        print(f'-- {broker_name} (PortfolioId: {portfolioId}) Broker Trades Done!')


if len(portfolio_df) > 0:
    try:
        portfolio_df["bourseAccount"] = portfolio_df["bourseAccount"].replace(
            {"رز": "رز ترنج"}, inplace=False, regex=False)
        crsr_rayan = powerbi_database.cursor()
        crsr_rayan.execute(f"TRUNCATE TABLE [nooredenadb].[brokers].[portfolio_rayan]")
        crsr_rayan.close()
        insert_to_database(portfolio_df, "[nooredenadb].[brokers].[portfolio_rayan]")
    except Exception as e:
        logger.error(e)

if len(trades_all_raw) > 0:
    trades_all_raw = trades_all_raw[trades_all_raw["comments"] != "نقل از قبل"].reset_index(drop=True)
    trades_all_raw["bourseAccount"] = trades_all_raw["bourseAccount"].replace(
        {"رز": "رز ترنج"}, inplace=False, regex=False)
    try:
        crsr_rayan = powerbi_database.cursor()
        crsr_rayan.execute(
            f"DELETE FROM [nooredenadb].[brokers].[trades_rayan] WHERE transactionDate >= '{start_date_rayan}'"
        )
        crsr_rayan.close()
        last_row = pd.read_sql(
            f"SELECT MAX(row_) as row_ FROM [nooredenadb].[brokers].[trades_rayan]", powerbi_database)["row_"].iloc[0]
        trades_all_raw["row_"] = range(last_row + 1, last_row + 1 + len(trades_all_raw))
        insert_to_database(dataframe=trades_all_raw, database_table="[nooredenadb].[brokers].[trades_rayan]")
    except Exception as e:
        logger.error(e)


d_ = thirty_days_ago.strftime("%Y/%m/%d")
query_ = (f"SELECT broker_id, portfolio_id, SUM(ABS(amount)) as amount FROM [nooredenadb].[brokers].[trades_rayan] "
          f"WHERE csTypeId in (1,2) and transactionDate>='{d_}' GROUP BY broker_id, portfolio_id")
last_month_trades = pd.read_sql(query_, powerbi_database)
for l in range(len(last_month_trades)):
    brokerId = int(last_month_trades["broker_id"].iloc[l])
    portfolioId = int(last_month_trades["portfolio_id"].iloc[l])
    value = int(last_month_trades["amount"].iloc[l])
    crsr_ = powerbi_database.cursor()
    sql = ("UPDATE [nooredenadb].[brokers].[brokers_balance] "
           "SET last_month_trades=? WHERE broker_id=? AND portfolio_id=?;")
    params = (value, brokerId, portfolioId)
    crsr_.execute(sql, params)
    crsr_.close()

###########################################################################

start_date_tadbir = pd.read_sql("SELECT MAX(TradeDate) as date FROM [nooredenadb].[brokers].[trades_tadbir]",
                                powerbi_database)["date"].iloc[0]
start_date_tadbir_ = start_date_tadbir[:10]

query_start_tadbir = "SELECT MAX(TransactionDate) as date FROM [nooredenadb].[brokers].[trades_tadbir_ledger]"
start_date_tadbir_ledger = pd.read_sql(query_start_tadbir, powerbi_database)["date"].iloc[0]
start_date_tadbir_ledger_ = start_date_tadbir_ledger[:10]
start_date_tadbir_ledger = jdatetime.datetime.fromgregorian(
    year=int(start_date_tadbir_ledger[:4]), month=int(start_date_tadbir_ledger[5:7]),
    day=int(start_date_tadbir_ledger[8:10])).strftime("%Y/%m/%d")

trades_all_raw = pd.DataFrame()
ledger_all_raw = pd.DataFrame()

for b in range(len(brokers_list)):
    if brokers_list["type_offline"].iloc[b] == "tadbir":
        broker_name = brokers_list["broker"].iloc[b]
        brokerId = int(brokers_list["broker_id"].iloc[b])
        portfolioId = int(brokers_list["portfolio_id"].iloc[b])
        print(f'-- {broker_name} ')
        try:
            broker = safe_login(
                broker_class=BrokersTadbirpardaz, url=brokers_list["address_offline"].iloc[b],
                username=brokers_list["username"].iloc[b], password=brokers_list["password"].iloc[b])
            safe_call_func(broker.get_ledger, from_date=start_date_tadbir_ledger, to_date=today)
            safe_call_func(broker.get_trades, from_date=start_date_tadbir_ledger, to_date=today)
            safe_call_func(broker.get_account_info)
            safe_call_func(broker.get_assets)

            trades = pd.DataFrame(broker.trades)
            if len(trades) > 0:
                trades = trades[['Symbol', 'CdsSymbol', 'TotalCommision', 'Volume', 'NetPrice', 'Price',
                                 'TradeDate', 'TradeId', 'TradeNumber', 'TradeSideTitle', 'VolumeInPrice']]
                trades["broker_id"] = brokerId
                trades["portfolio_id"] = portfolioId
                trades_all_raw = pd.concat([trades_all_raw, trades], axis=0, ignore_index=True)

            assets = pd.DataFrame(broker.assets)
            purchase_upper_bound = broker.account_info["Credit"] + broker.account_info["Remain"]
            assets_value = broker.assets_value
            remain = broker.account_info["Remain"]
            try:
                crsr = powerbi_database.cursor()
                sql = (
                    "UPDATE [nooredenadb].[brokers].[brokers_balance] SET purchase_upper_bound=?,remain=?,portfolio_value=? "
                    "WHERE broker_id=? AND portfolio_id=?; IF @@ROWCOUNT=0 BEGIN INSERT INTO "
                    "[nooredenadb].[brokers].[brokers_balance] (portfolio_id,broker_id,purchase_upper_bound,remain,"
                    "portfolio_value,last_month_trades,credit) VALUES (?, ?, ?, ?, ?, ?, ?); END")
                params = (
                    purchase_upper_bound, remain, assets_value, brokerId, portfolioId,
                    portfolioId, brokerId, purchase_upper_bound, remain, assets_value, 0, 0
                )
                crsr.execute(sql, params)
                crsr.close()
            except Exception as e:
                logger.error(e)

            ledger = pd.DataFrame(broker.ledger)
            if len(ledger) > 0:
                ledger.drop(columns=["TradeBranchDescription", "RowType", "DebitorBalance", "CreditorBalance", "Symbol",
                                     "AccountCode", "SubsidiaryLedgerTitle", "VoucherTypeCode"], inplace=True)
                ledger.replace({"MarketInstrumentTitle": {"": numpy.nan}, "MarketInstrumentISIN": {"": numpy.nan},
                                "BrokerStationCode": {"": numpy.nan}, "TradeNumber": {"": numpy.nan}},
                               inplace=True, regex=False)
                ledger["broker_id"] = brokerId
                ledger["portfolio_id"] = portfolioId
                ledger_all_raw = pd.concat([ledger_all_raw, ledger], axis=0, ignore_index=True)

        except Exception as e:
            logger.error(e)
        sleep(random.randint(10, 51)/10)

if len(trades_all_raw) > 0:
    try:
        crsr_rayan = powerbi_database.cursor()
        crsr_rayan.execute(
            f"DELETE FROM [nooredenadb].[brokers].[trades_tadbir] WHERE TradeDate >= '{start_date_tadbir_}'")
        crsr_rayan.close()
        insert_to_database(dataframe=trades_all_raw, database_table="[nooredenadb].[brokers].[trades_tadbir]")
    except Exception as e:
        logger.error(e)


ledger_all_raw = ledger_all_raw[
    ledger_all_raw["Description"] != "مانده نقل از قبل"].reset_index(drop=True, inplace=False)
if len(ledger_all_raw) > 0:
    try:
        crsr_rayan = powerbi_database.cursor()
        crsr_rayan.execute(
            f"DELETE FROM [nooredenadb].[brokers].[trades_tadbir_ledger] "
            f"WHERE TransactionDate >= '{start_date_tadbir_ledger_}'")
        crsr_rayan.close()
        last_row = pd.read_sql(f"SELECT MAX(row_) as row_ FROM [nooredenadb].[brokers].[trades_tadbir_ledger]",
                               powerbi_database)["row_"].iloc[0]
        ledger_all_raw["row_"] = range(last_row + 1, last_row + 1 + len(ledger_all_raw))
        insert_to_database(dataframe=ledger_all_raw, database_table="[nooredenadb].[brokers].[trades_tadbir_ledger]")
    except Exception as e:
        logger.error(e)

###########################################################################

d__ = thirty_days_ago.togregorian().strftime("%Y-%m-%d")
query_ = (f"SELECT broker_id, portfolio_id, SUM(ABS(NetPrice)) AS NetPrice FROM "
          f"[nooredenadb].[brokers].[trades_tadbir] WHERE TradeSideTitle IN ('خرید', 'فروش')"
          f" AND TradeDate>='{d__}' GROUP BY broker_id, portfolio_id")
last_month_trades = pd.read_sql(query_, powerbi_database)
for l in range(len(last_month_trades)):
    trades_value = int(last_month_trades["NetPrice"].iloc[l])
    brokerId = int(last_month_trades["broker_id"].iloc[l])
    portfolioId = int(last_month_trades["portfolio_id"].iloc[l])
    crsr_ = powerbi_database.cursor()
    sql = ("UPDATE [nooredenadb].[brokers].[brokers_balance] SET last_month_trades=? WHERE broker_id=? AND portfolio_id=?;")
    params = (trades_value, brokerId, portfolioId)
    crsr_.execute(sql, params)
    crsr_.close()

###########################################################################

project_path = str(Path(__name__).resolve().parent)
subprocess.run([sys.executable, "-m", "brokers.trades_cleaning"], cwd=project_path)

###########################################################################
