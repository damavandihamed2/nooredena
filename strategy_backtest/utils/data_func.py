import pandas as pd
import socket, typing, requests

from strategy_backtest.utils import database
from strategy_backtest.utils import tsetmc_api


##################################################

def db_conn_check():
    try:
        conn = socket.create_connection(("172.20.30.34", 1433), timeout=3)
        conn.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

##################################################

def get_market_indices(
        market_flow: typing.Literal[1, 2]
) -> list[str]:
    url_ = tsetmc_api.url_all_indices if market_flow == 1 else tsetmc_api.url_all_indices_farabourse
    headers_ = tsetmc_api.headers_all_indices if market_flow == 1 else tsetmc_api.headers_all_indices_farabourse
    res = requests.get(url=url_, headers=headers_)
    res_ = res.json()["indexB1"]
    return [asset["insCode"] for asset in res_]

def get_indices_all() -> list[str]:
    indices_id_list = []
    for m in [1, 2]:
        market_indices_list = get_market_indices(market_flow=m)
        indices_id_list += market_indices_list
    return indices_id_list

def asset_is_index(
        asset_id: str
) -> bool:
    indices_list = get_indices_all()
    if asset_id in indices_list:
        return True
    else:
        return False

##################################################

def get_index_data_tse(
        asset_id: str
) -> pd.DataFrame:
    response_ = requests.get(url=tsetmc_api.url_sector_history + f"{asset_id}",
                             headers=tsetmc_api.headers_sector_history,
                             data=tsetmc_api.payload_sector_history)
    response_df = pd.DataFrame(response_.json()["indexB2"])
    response_df = response_df[['dEven', 'xNivInuClMresIbs', 'xNivInuPbMresIbs', 'xNivInuPhMresIbs']]
    response_df.rename({"dEven": "date", "xNivInuClMresIbs": "close_price", "xNivInuPbMresIbs": "low_price",
                        "xNivInuPhMresIbs": "high_price"}, axis=1, inplace=True)
    response_df["open_price"] = response_df["close_price"].shift(1)
    response_df = response_df.dropna(subset=["open_price"], inplace=False, ignore_index=True)
    response_df.sort_values(by=["date"], inplace=True, ascending=True, ignore_index=True)
    response_df = response_df[["date", "open_price", "high_price", "low_price", "close_price"]]
    return response_df

def get_stock_data_tse(
        asset_id: str,
        adj_price: bool = False
) -> pd.DataFrame:
    response_ = requests.get(url=tsetmc_api.url_price_history + f"{asset_id}/0",
                             headers=tsetmc_api.headers_price_history,
                             data=tsetmc_api.payload_price_history)
    response_ = response_.json()["closingPriceDaily"]
    response_df = pd.DataFrame(response_)
    history = response_df[['priceMin', 'priceMax', 'priceYesterday', 'priceFirst', 'dEven', 'pClosing',
                               'pDrCotVal', 'zTotTran', 'qTotTran5J', 'qTotCap']]
    history.rename({'priceMin': "low_price", 'priceMax': "high_price", 'priceYesterday': "yesterday_price",
                        'priceFirst': "open_price", 'dEven': "date", 'pClosing': "close_price",
                        'pDrCotVal': "final_price", 'zTotTran': "trade_amount", 'qTotTran5J': "trade_volume",
                        'qTotCap': "trade_value"}, axis=1, inplace=True)
    history.sort_values(by=["date"], inplace=True, ascending=True, ignore_index=True)
    if adj_price:
        history["coef"] = (history["yesterday_price"].shift(-1) / history["final_price"]).fillna(1.0)
        history["adj_coef"] = history.iloc[::-1]["coef"].cumprod().iloc[::-1]
        history["open_price"] = round(history["open_price"] * history["adj_coef"])
        history["high_price"] = round(history["high_price"] * history["adj_coef"])
        history["low_price"] = round(history["low_price"] * history["adj_coef"])
        history["close_price"] = round(history["close_price"] * history["adj_coef"])
    history = history[["date", "open_price", "high_price", "low_price", "close_price", "trade_value"]]
    return history

def get_asset_history_tsetmc(
        asset_id: str,
        adj_price: bool = False
) -> pd.DataFrame:
    if asset_is_index(asset_id=asset_id):
        history = get_index_data_tse(asset_id=asset_id)
    else:
        history = get_stock_data_tse(asset_id=asset_id, adj_price=adj_price)
    return history

##################################################

def get_stock_data_database(
        asset_id: str,
        db_conn: database.pyodbc.connect,
        adj_price: bool = False
) -> pd.DataFrame:
    q_ = (f"SELECT date, open_price, high_price, low_price, close_price, yesterday_price, final_price, trade_value"
          f" FROM [nooredenadb].[tsetmc].[symbols_history] WHERE symbol_id='{asset_id}' ORDER BY date")
    history = pd.read_sql(q_, db_conn)
    if adj_price:
        history["coef"] = (history["yesterday_price"].shift(-1) / history["final_price"]).fillna(1.0)
        history["adj_coef"] = history.iloc[::-1]["coef"].cumprod().iloc[::-1]
        history["open_price"] = round(history["open_price"] * history["adj_coef"])
        history["high_price"] = round(history["high_price"] * history["adj_coef"])
        history["low_price"] = round(history["low_price"] * history["adj_coef"])
        history["close_price"] = round(history["close_price"] * history["adj_coef"])
    history = history[["date", "open_price", "high_price", "low_price", "close_price", "trade_value"]]
    return history

def get_index_data_database(
        asset_id: str,
        db_conn: database.pyodbc.connect
) -> pd.DataFrame:
    q_ = (f"SELECT date, high_price, low_price, close_price FROM [nooredenadb].[tsetmc].[indices_history] "
          f"WHERE indices_id='{asset_id}' ORDER BY date")
    history = pd.read_sql(q_, db_conn)
    history["open_price"] = history["close_price"].shift(1)
    history = history.dropna(subset=["open_price"], inplace=False, ignore_index=True)
    history = history[['date', 'open_price', 'high_price', 'low_price', 'close_price']]
    return history

def get_asset_history_database(
        asset_id: str,
        db_conn: database.pyodbc.connect,
        adj_price: bool = False
) -> pd.DataFrame:
    if asset_is_index(asset_id=asset_id):
        history = get_index_data_database(
            asset_id=asset_id,
            db_conn=db_conn
        )
    else:
        history = get_stock_data_database(
            asset_id=asset_id,
            db_conn=db_conn,
            adj_price=adj_price
        )
    return history

##################################################

def get_asset_history(asset_id: str, adj_price: bool = False):
    if db_conn_check():
        db_conn_public = database.db_conn_public
        history = get_asset_history_database(
            asset_id=asset_id,
            db_conn=db_conn_public,
            adj_price=adj_price
        )
    else:
        history = get_asset_history_tsetmc(
            asset_id=asset_id,
            adj_price=adj_price
        )
    return history

##################################################

def get_index_symbols(index_id: str) -> pd.DataFrame :
    response = requests.get(f"https://cdn.tsetmc.com/api/ClosingPrice/GetIndexCompany/{index_id}")
    response = pd.DataFrame(response.json()["indexCompany"])["instrument"].tolist()
    companies = pd.DataFrame(response)
    companies = companies[["lVal18AFC", "insCode"]].rename(
        columns={"lVal18AFC": "symbol", "insCode": "symbol_id"}, inplace=False)
    return companies

##################################################

def get_symbols_data() -> pd.DataFrame:
    db_conn = database.db_conn_public
    q_ = ("SELECT symbol, symbol_id, sector, flow, flow_name, yval, total_share, final_price, "
          "(total_share * final_price) AS market_cap FROM [nooredenadb].[tsetmc].[symbols] "
          "WHERE active=1 AND final_last_date >= 20250601")
    symbols = pd.read_sql(q_, db_conn)
    return symbols

##################################################

def get_wr_funds_data(funds_name: typing.Literal["pishtazfund", "pishrofund"]) -> pd.DataFrame :
    response = requests.get(url=f"https://{funds_name}.com/Chart/TotalNAV?type=getnavtotal", verify=False)
    response_json = response.json()
    fund_data = pd.DataFrame(columns=["date"])
    for i in range(len(response_json)):
        response_df = pd.DataFrame(response_json[i]["List"])
        response_df = response_df[["x", "y"]]
        response_df.rename(columns={"x": "date", "y": response_json[i]["name"]}, inplace=True)
        fund_data = fund_data.merge(response_df, how="outer", on="date")
    fund_data["date"] = pd.to_datetime(fund_data["date"], format="%m/%d/%Y").dt.strftime("%Y%m%d").astype(int)
    fund_data.rename(
        mapper={"صدور": "write_price", "آماری": "statistical_price", "ابطال": "revoke_price"}, axis=1, inplace=True)
    fund_data.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
    return fund_data

##################################################
##################################################
##################################################


if __name__ == "__main__":

    asset_id_signal = "32097828799138957"  # Total Index
    asset_id_trade = "17914401175772326"  # Ahrom Fund

    tmp_signal = get_asset_history(asset_id=asset_id_signal)
    tmp_trade = get_asset_history(asset_id=asset_id_trade)

    pishtaz_data = get_wr_funds_data(funds_name="pishtazfund")
    pishro_data = get_wr_funds_data(funds_name="pishrofund")




