import pandas as pd
import os, sys, warnings, jdatetime

from utils.custom_logger import get_logger
from utils.database import make_connection, insert_to_database
from brokers.coinonline.coinonline import CoinOnline



db_conn = make_connection()
warnings.filterwarnings("ignore")
try:
    filename = os.path.basename(__file__)
except:
    filename = "brokers_trades.py"
logger = get_logger(db_connection=db_conn, file_name=filename,
                    project_dir=os.path.dirname(os.path.abspath(sys.argv[0])))

today = jdatetime.date.today().strftime('%Y/%m/%d')
brokers_list = pd.read_sql(
    "SELECT bc.broker_id, bc.portfolio_id, bc.username, bc.password, b.broker, b.type_online, "
    "b.address_ime FROM [nooredenadb].[brokers].[brokers_credential] bc LEFT JOIN "
    "[nooredenadb].[brokers].[brokers] b ON bc.broker_id = b.broker_id WHERE bc.system_type=1 "
    "AND bc.active=1 AND bc.market_type=2",

    db_conn
)

last_date_trades = pd.read_sql(
    "SELECT Max(date) FROM [nooredenadb].[brokers].[ime_trades_coinonline]", db_conn)[""].iloc[0]
last_date_statements = pd.read_sql(
    "SELECT Max(date) FROM [nooredenadb].[brokers].[ime_statements_coinonline]", db_conn)[""].iloc[0]


trades_df = pd.DataFrame()
statements_df = pd.DataFrame()
for b in range(len(brokers_list)):
    portfolio_id = brokers_list["portfolio_id"].iloc[b]
    broker_name, broker_id = brokers_list["broker"].iloc[b], brokers_list["broker_id"].iloc[b]
    print(f'-- {broker_name} (PortfolioId: {portfolio_id})')

    agent = CoinOnline(address=brokers_list["address_ime"].iloc[b], portfolio_id=portfolio_id,
                       username=brokers_list["username"].iloc[b], password=brokers_list["password"].iloc[b])
    agent.login()

    trades_tmp = agent.get_trades(start_date=last_date_trades, end_date=today, page_size=100)
    trades_tmp_df = pd.DataFrame(trades_tmp)
    trades_tmp_df["portfolio_id"] = portfolio_id
    trades_tmp_df["broker_id"] = broker_id
    trades_df = pd.concat([trades_df, trades_tmp_df], axis=0, ignore_index=True)

    statements_tmp = agent.get_statements(start_date=last_date_statements, end_date=today, page_size=100)
    statements_tmp_df = pd.DataFrame(statements_tmp)
    statements_tmp_df["portfolio_id"] = portfolio_id
    statements_tmp_df["broker_id"] = broker_id
    statements_df = pd.concat([statements_df, statements_tmp_df], axis=0, ignore_index=True)


trades_df = trades_df[["date", "time", "trade_id", "symbol", "trade_type",
                       "price", "volume", "status", "portfolio_id", "broker_id"]]
if not trades_df.empty:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[ime_trades_coinonline] WHERE date >= '{last_date_trades}'")
    insert_to_database(trades_df, "[nooredenadb].[brokers].[ime_trades_coinonline]")


statements_df = statements_df[['date', 'time', 'document_id', 'symbol', 'description',
                               'debtor', 'creditor', 'portfolio_id', 'broker_id']]
if not statements_df.empty:
    crsr = db_conn.cursor()
    crsr.execute(f"DELETE FROM [nooredenadb].[brokers].[ime_statements_coinonline] WHERE date >= '{last_date_statements}'")
    insert_to_database(statements_df, "[nooredenadb].[brokers].[ime_statements_coinonline]")

# SELECT
#     portfolio_id, date, symbol, trade_type, sum(volume) volume, sum(volume * price) value
# FROM [nooredenadb].[brokers].[ime_trades_coinonline]
# GROUP BY
#     portfolio_id, date, symbol, trade_type
# ORDER BY
#     date
