import pandas as pd
from pyodbc import Connection



def get_dim_date(db_connection: Connection) -> pd.DataFrame:
    query3 = ("SELECT [Jalali_1] as date, [Miladi] as date_ FROM [nooredenadb].[extra].[dim_date] "
              "WHERE Jalali_1 >= '1395/01/01' AND Jalali_1 <= '1410/08/30'")
    dim_date = pd.read_sql(query3, db_connection)
    dim_date["date_"] = dim_date["date_"].astype("str")
    return dim_date


def get_remaining_rayan(db_connection: Connection) -> pd.DataFrame:
    query1 = ("SELECT trades_rayan.row_, trades_rayan.transactionDate as date, trades_rayan.remaining, "
              "trades_rayan.broker_id, trades_rayan.portfolio_id FROM [nooredenadb].[brokers].[trades_rayan]"
              " INNER JOIN (SELECT max(row_) as row__, transactionDate as date, broker_id, portfolio_id FROM "
              "[nooredenadb].[brokers].[trades_rayan] group by transactionDate, broker_id, portfolio_id)"
              " as temp ON row__=row_ ORDER BY date, broker_id, portfolio_id")
    credits_rayan = pd.read_sql(query1, db_connection)
    credits_rayan.drop(columns=["row_"], inplace=True)
    credits_rayan.sort_values(by=["broker_id", "portfolio_id", "date"], inplace=True)
    return credits_rayan


def get_remaining_tadbir(db_connection: Connection) -> pd.DataFrame:
    query2 = ("SELECT temp.row_, temp.broker_id, temp.portfolio_id, temp.date_, trades_tadbir_ledger.Remain remaining"
              " FROM (SELECT max(row_) row_, broker_id, portfolio_id, substring(TransactionDate, 1, 10) date_ FROM "
              "[nooredenadb].[brokers].[trades_tadbir_ledger] WHERE Description "
              "NOT IN ('سند اختتامیه', 'سند افتتاحیه مورخ {0}') group by substring(TransactionDate, 1, 10), "
              "broker_id, portfolio_id) temp INNER JOIN [nooredenadb].[brokers].[trades_tadbir_ledger] "
              "ON temp.row_ = trades_tadbir_ledger.row_")
    credits_tadbir = pd.read_sql(query2, db_connection)
    dim_date = get_dim_date(db_connection=db_connection)
    credits_tadbir = credits_tadbir.merge(dim_date, on="date_", how="left")
    credits_tadbir = credits_tadbir.drop(columns=["date_", "row_"], inplace=False)
    return credits_tadbir


def get_credits_history(start_date: str, end_date: str, db_connection: Connection) -> pd.DataFrame:
    dim_date = get_dim_date(db_connection=db_connection)
    remaining_rayan = get_remaining_rayan(db_connection=db_connection)
    remaining_tadbir = get_remaining_tadbir(db_connection=db_connection)
    credits = pd.concat([remaining_rayan, remaining_tadbir], axis=0, ignore_index=True)

    full_date_broker = pd.MultiIndex.from_product([
        dim_date["date"].drop_duplicates(inplace=False).sort_values(inplace=False),
        credits["broker_id"].drop_duplicates(inplace=False),
        credits["portfolio_id"].drop_duplicates(inplace=False)],
        names=['date', 'broker_id', "portfolio_id"])

    df_result = credits.set_index(
        keys=['date', 'broker_id', "portfolio_id"], inplace=False, drop=True
    ).reindex(
        full_date_broker
    ).reset_index(
        drop=False, inplace=False
    ).sort_values(
        by=['broker_id', "portfolio_id", 'date'], ignore_index=True, inplace=False
    )

    df_result['remaining'] = df_result.groupby(by=["portfolio_id", 'broker_id'])['remaining'].ffill().fillna(0)
    df_result["remaining"] = ((df_result["remaining"] < 0) * 1) * df_result["remaining"]

    df_result = df_result.groupby(by='date', as_index=False)['remaining'].sum()
    df_result = df_result[(df_result["date"] >= start_date) & (df_result["date"] <= end_date)]
    df_result = df_result.sort_values(by="date", inplace=False, ignore_index=True)
    df_result.rename(columns={"remaining": "credit"}, inplace=True)

    return df_result



import warnings
from utils.database import make_connection

warnings.filterwarnings("ignore")
db_conn = make_connection()
start_date = "1404/10/30"
end_date = "1405/06/31"
credits_history_df = get_credits_history(start_date=start_date, end_date=end_date, db_connection=db_conn)
