from typing import Any
import pandas as pd
from utils.database import make_connection



def get_symbol_return(
        history,
        start_date: str,
        end_date: str
) -> dict[str, Any]:

    if end_date <= start_date:
        raise ValueError("End date must be greater than start date")

    history = history[(history['date']>start_date) & (history['date']<end_date)]
    if len(history)==0:
        return
    history["coef"] = (history["yesterday_price"].shift(-1) / history["final_price"]).fillna(1.0)
    history["adj_coef"] = history.iloc[::-1]["coef"].cumprod().iloc[::-1]
    history["final_price_adj"] = round(history["final_price"] * history["adj_coef"])
    symbol_return =  float(100 * ((history["final_price_adj"].iloc[-1] / history["final_price_adj"].iloc[0]) - 1))

    return symbol_return


def get_return(
        history,
        agg_raw,
        start_sate: int,
        end_date: int
):
    agg = agg_raw.copy()
    company_ids = agg['company_id'].unique()
    for company_id in company_ids:
        hist = history[history["symbol_id"]==company_id].sort_values(by=['date'], ascending=False).reset_index(drop=True)
        agg.loc[(agg['company_id']==company_id), 'company_return'] = \
            get_symbol_return(
                company_id,
                hist,
                (start_sate - 1) * 100, (end_date + 1) * 100
            )
    return agg




start_date = 20250630
end_date = 20260630
symbol_id = "46348559193224090"
db_conn = make_connection()
history = pd.read_sql(f"select * from [nooredenadb].[tsetmc].[symbols_history] WHERE symbol_id='{symbol_id}' ORDER BY date asc", db_conn)

if end_date <= start_date:
    raise ValueError("End date must be greater than start date")
history = history[(history['date'] > start_date) & (history['date'] < end_date)]
# print(len(history))

history["coef"] = (history["yesterday_price"].shift(-1) / history["final_price"]).fillna(1.0)
history["adj_coef"] = history.iloc[::-1]["coef"].cumprod().iloc[::-1]
history["final_price_adj"] = round(history["final_price"] * history["adj_coef"])
symbol_return = float(
    100 * (
            (
                    history["final_price_adj"].iloc[-1] / history["final_price_adj"].iloc[0]
            ) - 1
    )
)


