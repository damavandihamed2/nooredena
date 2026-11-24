import pandas as pd
from pyodbc import Connection



def get_broker_cashflow(broker_type: str, db_connection: Connection) -> pd.DataFrame:

    if broker_type == "tadbir":
        query = """
            SELECT dd.Jalali_1 date, ttl.portfolio_id, ttl.Debitor out, ttl.Creditor [in] FROM [nooredenadb].[brokers].[trades_tadbir_ledger] ttl
            LEFT JOIN
            [nooredenadb].[extra].[dim_date] dd
            ON ttl.TransactionDate = dd.Miladi
            WHERE
                ((ttl.Description LIKE '%حواله%') OR (ttl.Description LIKE '%دریافت%') OR (ttl.Description LIKE '%پرداخت%') OR (ttl.Description LIKE '%واریز%'))
            AND
            (ttl.MarketInstrumentTitle IS NULL) AND ((ttl.Description NOT LIKE '%بهره%') AND (ttl.Description NOT LIKE '%سود%') AND (ttl.Description NOT LIKE '%اختیار%'))
        """

    elif broker_type == "rayan":
        query = """
             SELECT transactionDate date, portfolio_id, -debtor out, creditor [in] FROM nooredenadb.brokers.trades_rayan
            WHERE
                csTypeId IN (3, 4)
                AND
                ((rowOrder LIKE '42%' OR rowOrder LIKE '51%' OR rowOrder LIKE '52%' OR rowOrder LIKE '54%')
                OR
                ((rowOrder LIKE '53%') AND (comments LIKE '%پاياياي%' OR comments LIKE '%پایاپای%')))
             """

    else:
        raise ValueError(f"Invalid broker type: {broker_type} (broker_type should be 'rayan' OR 'tadbir')")

    cashflow = pd.read_sql_query(query, db_connection)

    return cashflow


def get_cashflow(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    rayan_cashflow = get_broker_cashflow(broker_type="rayan", db_connection=db_connection)
    tadbir_cashflow = get_broker_cashflow(broker_type="tadbir", db_connection=db_connection)

    cashflow = pd.concat([rayan_cashflow, tadbir_cashflow], axis=0, ignore_index=True)
    cashflow = cashflow.groupby("date", as_index=False)[["in", "out"]].sum()
    cashflow= cashflow[(cashflow["date"] >= start_date) & (cashflow["date"] <= end_date)]
    cashflow = cashflow.sort_values(by="date", ignore_index=True)

    return cashflow

