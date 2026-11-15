import pandas as pd
from pyodbc import Connection



def get_dim_date(db_connection: Connection) -> pd.DataFrame:
    q_ = (f"SELECT Jalali_1 date, MAX(Jalali_1) OVER (PARTITION BY LEFT(Jalali_1, 7)) AS monthly FROM "
          f"[nooredenadb].[extra].[dim_date] ORDER BY Miladi;")
    dim_date = pd.read_sql(q_, db_connection)
    return dim_date


def get_broker_credit_cost(broker_type: str, db_connection: Connection) -> pd.DataFrame:

    if broker_type == "tadbir":
        query = """
        SELECT ttl.Debitor cost, ttl.broker_id, dd.Jalali_1 date FROM [nooredenadb].[brokers].[trades_tadbir_ledger] ttl
        LEFT JOIN
        [nooredenadb].[extra].[dim_date] dd
        ON ttl.TransactionDate = dd.Miladi 
        WHERE
            ttl.MarketInstrumentTitle IS Null AND
            ((ttl.Description LIKE '%هزینه%' AND ttl.Description LIKE '%تسهیلات%') OR (ttl.Description LIKE '%هزینه%' AND ttl.Description LIKE '%مالی%'))
        """

    elif broker_type == "rayan":
        query = """
             SELECT transactionDate date, broker_id, -debtor cost
             FROM nooredenadb.brokers.trades_rayan
             WHERE csTypeName = 'پرداخت' AND rowOrder LIKE N'%هزينه بهره مربوط به کسري وجوه%'
             ORDER BY date, broker_id \
             """

    else:
        raise ValueError(f"Invalid broker type: {broker_type} (broker_type should be 'rayan' OR 'tadbir')")

    credit_cost = pd.read_sql_query(query, db_connection)

    dim_date = get_dim_date(db_connection)
    dim_date["date"] = dim_date["date"].shift(-5)
    credit_cost = credit_cost.merge(dim_date, on="date", how="left")
    credit_cost = credit_cost.drop(columns="date").rename(columns={"monthly": "date"}, inplace=False)
    return credit_cost



def get_credit_cost(db_connection: Connection) -> pd.DataFrame:

    rayan_cost = get_broker_credit_cost(broker_type="rayan", db_connection=db_connection)
    tadbir_cost = get_broker_credit_cost(broker_type="tadbir", db_connection=db_connection)

    credit_cost = pd.concat([rayan_cost, tadbir_cost], axis=0, ignore_index=True)
    credit_cost = credit_cost.groupby("date", as_index=False)["cost"].sum()
    credit_cost = credit_cost.sort_values(by="date", ignore_index=True)

    return credit_cost



if __name__ == "__main__":

    import warnings
    from utils.database import make_connection

    warnings.filterwarnings("ignore")
    db_conn = make_connection()

    if db_conn:
        credit_costs_df = get_credit_cost(db_connection=db_conn)
        db_conn.close()
