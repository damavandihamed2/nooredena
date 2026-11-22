import pandas as pd

from utils.database import make_connection, insert_to_database




db_conn = make_connection()


trades_ime = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[trades_ime] WHERE symbol IN (SELECT ContractCode FROM nooredenadb.ime.live_tablo_cdc)", db_conn)
trades_ime = trades_ime.groupby(by=['date', 'portfolio_id', 'symbol', 'type'], as_index=False).sum()

trades_ime["value"] = trades_ime["value"] + trades_ime["commission"] * (trades_ime["type"] * -2) + 3
trades_ime.to_excel("c:/users/h.damavandi/desktop/trades_ime.xlsx", index=False)



q = """
    DECLARE @DaysBack INT = 30;
    DECLARE @StartDate DATE = DATEADD(DAY, -@DaysBack, CAST(GETDATE() AS DATE));
    
    WITH CombinedTrades AS (
    
    -- Trades Tadbir Ledger
        SELECT
            dd.Jalali_1 AS [تاریخ],
            CASE WHEN LEN(tt.Symbol) > 0
                 THEN LEFT(tt.Symbol, LEN(tt.Symbol) - 1)
                 ELSE tt.Symbol
            END AS [نماد],
            ttl.Description AS [شرح],
            ABS(ttl.Debitor) AS [بدهکار],
            ABS(ttl.Creditor) AS [بستانکار],
            ttl.Remain AS [مانده],
            ps.portfolio_name AS [سبد],
            b.broker_name AS [کارگزاری],
            ttl.portfolio_id,
            ttl.broker_id,
            ttl.row_ AS SortRow
        FROM nooredenadb.brokers.trades_tadbir_ledger ttl
        LEFT JOIN nooredenadb.brokers.trades_tadbir AS tt
            ON 	ttl.broker_id=tt.broker_id AND 
                SUBSTRING(tt.TradeDate, 1, 10) = SUBSTRING(ttl.TransactionDate, 1, 10) AND 
                ttl.TradeNumber=tt.TradeNumber
        LEFT JOIN nooredenadb.brokers.portfolio_specifics ps
            ON ttl.portfolio_id = ps.portfolio_id
        LEFT JOIN nooredenadb.brokers.brokers b
            ON ttl.broker_id = b.broker_id
        LEFT JOIN nooredenadb.extra.dim_date dd
            ON SUBSTRING(ttl.TransactionDate, 1, 10) = dd.Miladi
        WHERE TRY_CONVERT(DATE, ttl.TransactionDate) >= @StartDate
    
        UNION ALL
    
        -- Trades Rayan
        SELECT
            dd.Jalali_1 AS [تاریخ],
            CASE
                WHEN tr.[bourseAccount] LIKE N'ذ%'
                AND NOT EXISTS (SELECT 1 FROM [nooredenadb].[tsetmc].[symbols] s WHERE s.[symbol] = tr.[bourseAccount])
                THEN SUBSTRING(tr.[bourseAccount], 2, 2147483647)
                ELSE tr.[bourseAccount]
            END AS [نماد],
            tr.comments AS [شرح],
            ABS(tr.debtor) AS [بدهکار],
            ABS(tr.creditor) AS [بستانکار],
            tr.remaining AS [مانده],
            ps.portfolio_name AS [سبد],
            b.broker_name AS [کارگزاری],
            tr.portfolio_id,
            tr.broker_id,
            tr.row_ AS SortRow
        FROM nooredenadb.brokers.trades_rayan tr
        LEFT JOIN nooredenadb.brokers.portfolio_specifics ps
            ON tr.portfolio_id = ps.portfolio_id
        LEFT JOIN nooredenadb.brokers.brokers b
            ON tr.broker_id = b.broker_id
        LEFT JOIN nooredenadb.extra.dim_date dd
            ON tr.transactionDate = dd.Jalali_1
        WHERE dd.Miladi >= @StartDate
    
    )
    
    SELECT
        ROW_NUMBER() OVER (ORDER BY [تاریخ], portfolio_id, broker_id, SortRow) AS [ردیف],
        [تاریخ],
        [نماد],
        [شرح],
        [بدهکار],
        [بستانکار],
        [مانده],
        [سبد],
        [کارگزاری]
    FROM CombinedTrades
    WHERE 
        CombinedTrades.[تاریخ] < (SELECT MAX([تاریخ]) FROM CombinedTrades)
    ORDER BY
        [تاریخ],
        portfolio_id,
        broker_id,
        SortRow;
    """


brokers_df = pd.read_sql(q, db_conn)
brokers_df.to_excel("brokers_v3.xlsx", index=False)