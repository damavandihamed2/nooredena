import utils.database
import warnings
import jdatetime
import pandas as pd



warnings.filterwarnings("ignore")
db_conn = database.db_conn
one_day = jdatetime.timedelta(days=1)
today = jdatetime.datetime.today()
today_s = today.strftime("%Y/%m/%d")

symbols = pd.read_sql("SELECT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols]", db_conn)
symbols_history = pd.read_sql("SELECT date, symbol_id, final_price FROM [nooredenadb].[tsetmc].[symbols_history] "
                              "WHERE trade_amount > 0", db_conn)


if (today + one_day).month == today.month:
    month = today.month - 1 if today.month - 1 != 0 else 12
    year = today.year - 1 if today.month - 1 != 0 else today.year - 2
    day = 31 if month in [1, 2, 3, 4, 5, 6] else 30
    if month == 12:
        if not jdatetime.datetime.strptime(f"{year}", "%Y").isleap(): day = 29
    start_date = f"{year}/{month:02d}/{day:02d}"
else:
    month = today.month
    year = today.year - 1
    day = 31 if month in [1, 2, 3, 4, 5, 6] else 30
    if month == 12:
        if not jdatetime.datetime.strptime(f"{year}", "%Y").isleap():
            day = 29
    start_date = f"{year}/{month:02d}/{day:02d}"




trades_df = pd.read_sql(f"SELECT * FROM [nooredenadb].[company].[trades] WHERE date > '{start_date}'", db_conn)
dividend_df = pd.read_sql(f"SELECT * FROM [nooredenadb].[company].[dividend] WHERE date > '{start_date}'", db_conn)
portfolio_df = pd.read_sql(f"SELECT * FROM [nooredenadb].[company].[portfolio_history] WHERE date >= '{start_date}'", db_conn)

symbols_ = symbols[symbols["symbol"].isin(portfolio_df["symbol"].unique())]



"""

SELECT date, portfolio_history.symbol, symbol_id FROM [nooredenadb].[company].[portfolio_history]
LEFT JOIN
[nooredenadb].[tsetmc].[symbols]
ON portfolio_history.symbol=symbols.symbol
 WHERE date >= '1403/05/31'
 ORDER BY symbol, date

SELECT symbol, symbol_id FROM [nooredenadb].[tsetmc].[symbols]

SELECT date, symbol_id, final_price FROM [nooredenadb].[tsetmc].[symbols_history] WHERE trade_amount > 0


"""