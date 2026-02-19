import pandas as pd
import requests as rq
from tqdm import tqdm
from time import sleep
import random, warnings

from tsetmc import tsetmc_api
from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
open_price_zero = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols_history] where open_price=0 and trade_amount!=0 order by date", db_conn)

if len(open_price_zero) > 0:
    ids_ = open_price_zero["symbol_id"].unique().tolist()
    for i in tqdm(range(len(ids_))):
        idx_ = ids_[i]
        dates = open_price_zero[open_price_zero["symbol_id"] == idx_]["date"].unique().tolist()
        response = rq.get(url=tsetmc_api.url_price_history + idx_ + "/0", headers=tsetmc_api.headers_price_history, data=tsetmc_api.payload_price_history)
        sleep(random.randint(100, 301) / 100)
        his_df = pd.DataFrame(response.json()["closingPriceDaily"])
        for d in range(len(dates)):
            op_price = int(his_df[his_df["dEven"] == dates[d]].reset_index(drop=True, inplace=False)["priceFirst"].iloc[0])
            if op_price == 0:
                trd_amont = int(his_df[his_df["dEven"] == dates[d]].reset_index(drop=True, inplace=False)["zTotTran"].iloc[0])
                if trd_amont == 0:
                    crsr = db_conn.cursor()
                    crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols_history] SET trade_amount=0, trade_volume=0, trade_value=0 WHERE symbol_id='{idx_}' and date={dates[d]};")
                    crsr.close()
                else:
                    res = rq.get(url=f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDaily/{idx_}/{dates[d]}", headers=tsetmc_api.headers_price_history)
                    sleep(random.randint(100, 301) / 100)
                    op_price = int(res.json()["closingPriceDaily"]["priceFirst"])
                    if op_price != 0:
                        crsr = db_conn.cursor()
                        crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols_history] SET open_price={op_price} WHERE symbol_id='{idx_}' and date={dates[d]};")
                        crsr.close()
                    else:
                        res_ = rq.get(f"https://cdn.tsetmc.com/api/Trade/GetTradeHistory/{idx_}/{dates[d]}/false", headers=tsetmc_api.headers_price_history)
                        sleep(random.randint(100, 301) / 100)
                        res_ = pd.DataFrame(res_.json()["tradeHistory"])
                        res__ = res_[res_["canceled"] != 1]
                        if len(res__) > 0:
                            op_price = int(res__.sort_values("nTran", inplace=False, ignore_index=True)["pTran"].iloc[0])
                            crsr = db_conn.cursor()
                            crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols_history] SET open_price={op_price} WHERE symbol_id='{idx_}' and date={dates[d]};")
                            crsr.close()
                        else:
                            crsr = db_conn.cursor()
                            crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols_history] SET trade_amount=0, trade_volume=0, trade_value=0 WHERE symbol_id='{idx_}' and date={dates[d]};")
                            crsr.close()
            else:
                crsr = db_conn.cursor()
                crsr.execute(f"UPDATE [nooredenadb].[tsetmc].[symbols_history] SET open_price={op_price} WHERE symbol_id='{idx_}' and date={dates[d]};")
                crsr.close()
else:
    pass
