import pandas as pd
import warnings, jdatetime
from typing import Literal, Union

from utils import database


warnings.filterwarnings("ignore")
db_conn = database.make_connection()


def add_one_day_j(date: str):
    return (jdatetime.datetime.strptime(date, "%Y/%m/%d") + jdatetime.timedelta(days=1)).strftime("%Y/%m/%d")

def get_last_board_meeting_date():
    last_date = pd.read_sql("SELECT MAX(date) AS date FROM [nooredenadb].[extra].[board_meeting_dates]", db_conn)
    last_date = last_date["date"].iloc[0]
    return last_date


def get_last_trade_date():
    last_date = pd.read_sql("SELECT TOP(1) date FROM [nooredenadb].[brokers].[trades_last]", db_conn)
    if last_date.empty:
        last_date = pd.read_sql("SELECT MAX(date) as date FROM [nooredenadb].[company].[trades]", db_conn)
    last_date = last_date["date"].iloc[0]
    return last_date


def check_basket(l: set):
    check_1, check_2 =  1 in l, bool(l & {25, 26, 27, 28})
    if check_1 and check_2: return "هر دو"
    elif check_1: return "اصلی"
    elif check_2: return "prx"
    else: return "نامشخص"


def get_avalhami_data(date: str = "last") -> pd.DataFrame:
    query_avalhami_raw = ("SELECT [JalaliDate] date, [SellNAVPerShare] final_price, ISNULL(TEMP.funds_unit, 0) amount, "
                          "ISNULL(TEMP.cost, 0) total_cost FROM [nooredenadb].[extra].[avalhami_nav] LEFT JOIN (SELECT date,"
                          " SUM((CASE WHEN type=1 THEN funds_unit ELSE funds_unit * -1 END)) funds_unit, SUM((CASE WHEN "
                          "type=1 THEN value ELSE cost * -1 END)) AS cost FROM [nooredenadb].[extra].[avalhami_trades] "
                          "GROUP BY date) AS TEMP ON avalhami_nav.JalaliDate=TEMP.date ORDER by avalhami_nav.JalaliDate")
    avalhami_df = pd.read_sql(query_avalhami_raw, db_conn)
    if date != "last":
        avalhami_df = avalhami_df[avalhami_df["date"] <= date].reset_index(drop=True)
    avalhami_df = pd.DataFrame([{
        "portfolio_id": 1, "symbol": "حامی اول", "amount": avalhami_df["amount"].sum(),
        "total_cost": avalhami_df["total_cost"].sum(), "total_cost_sep": avalhami_df["total_cost"].sum(),
        "final_price": avalhami_df["final_price"].iloc[-1]}])
    return avalhami_df


def portfolio_date_time() -> (str, str):
    q_ = "SELECT TOP(1) date, time FROM [nooredenadb].[portfolio].[portfolio_daily_report_diff] WHERE date IS NOt NULL"
    date_time = pd.read_sql(q_, db_conn)
    date, time = date_time["date"].iloc[0].replace('-', '/'), date_time["time"].iloc[0]
    return date, time


def get_symbols_data() -> pd.DataFrame:
    query_symbols = ("select symbol, total_share, sector_name from [nooredenadb].[tsetmc].[symbols]"
                     " ORDER BY symbol, active, final_last_date")
    symbols = pd.read_sql(query_symbols, db_conn)
    symbols.drop_duplicates(subset="symbol", keep="last", ignore_index=True, inplace=True)
    return symbols


def get_fix_income_symbols() -> pd.DataFrame:
    query_fix_income_symbols = "SELECT [symbol] FROM [nooredenadb].[tsetmc].[symbols] WHERE subsector='6812'"
    fix_income_symbols = pd.read_sql(query_fix_income_symbols, db_conn)
    return fix_income_symbols


def get_portfolio(
        date: str = "last",
        include_avalhami: bool = True,
        main_portfolio: bool = True,
        prx_portfolio: bool = True
) -> pd.DataFrame:

    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    if date == "last":
        query_portfolio = ("SELECT portfolio_temp.portfolio_id, portfolio_temp.symbol, portfolio_temp.amount, "
                           "portfolio_temp.total_cost, portfolio_temp.total_cost_sep, d.close_price final_price FROM "
                           "[nooredenadb].[portfolio].[portfolio_temp] LEFT JOIN (SELECT distinct(symbol), close_price "
                           "FROM [nooredenadb].[portfolio].[portfolio_daily_report_diff]) d "
                           "ON portfolio_temp.symbol=d.symbol WHERE portfolio_temp.symbol != 'همراه'")
        date_, time_ = portfolio_date_time()
    else:
        query_portfolio = (f"SELECT portfolio_id, symbol, volume amount, total_cost, total_cost_sep, final_price FROM"
                           f" nooredenadb.company.portfolio_history WHERE date='{date}'")
        date_, time_ = date, "12:30"

    portfolio = pd.read_sql(query_portfolio, db_conn)
    symbols = get_symbols_data()[["symbol", "total_share"]]

    if main_portfolio:
        if include_avalhami:
            portfolio = pd.concat([portfolio, get_avalhami_data(date=date)], axis=0, ignore_index=True)
        if not prx_portfolio:
            portfolio = portfolio[portfolio["portfolio_id"] == 1].reset_index(drop=True)
        portfolio.drop(labels=["total_cost_sep"], axis=1, inplace=True)

    if (not main_portfolio) and prx_portfolio:
        portfolio = portfolio[portfolio["portfolio_id"] != 1].reset_index(drop=True)
        portfolio.drop(labels=["total_cost"], axis=1, inplace=True)
        portfolio.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    portfolio = portfolio.groupby(by=["portfolio_id", "symbol", "final_price"], as_index=False).sum()
    symbols_basket = pd.DataFrame(portfolio.groupby("symbol")["portfolio_id"].apply(set).apply(
        check_basket)).reset_index(drop=False, names="symbol").rename({"portfolio_id": "basket"}, axis=1, inplace=False)
    portfolio = portfolio[["symbol", "final_price", "amount", "total_cost"]].groupby(
        by=["symbol", "final_price"], as_index=False).sum().merge(symbols_basket, on="symbol", how="left")
    portfolio["cost_per_share"] = portfolio["total_cost"] / portfolio["amount"]
    portfolio = portfolio.merge(symbols, on="symbol", how="left")
    portfolio["ownership"] = portfolio["amount"] / portfolio["total_share"]
    portfolio.drop(columns=["total_share"], inplace=True)
    portfolio["value"] = portfolio["amount"] * portfolio["final_price"]
    portfolio.sort_values(["value"], ignore_index=True, inplace=True, ascending=False)
    portfolio["share_of_portfo"] = portfolio["value"] / portfolio["value"].sum()
    portfolio["profitloss"] = portfolio["value"] - portfolio["total_cost"]
    portfolio["profitloss_percent"] = portfolio["profitloss"] / portfolio["total_cost"]
    portfolio["date"] = date_
    portfolio["time"] = time_
    return portfolio


########################################################################################################################


def get_trades(trade_type: Literal[1, 2], start_date: str, end_date: str, fix_exclude:bool = True,
               main_portfolio: bool = True, prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:

    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    cols = ["symbol", "basket", "volume", "mean_price", "value", "share_percent"]
    query_trades = ("SELECT portfolio_id, symbol, is_ros, is_paid_ros, SUM(volume) volume, SUM(value) value, "
                    "SUM(total_cost) total_cost, SUM(total_cost_sep) total_cost_sep FROM (SELECT date, type, "
                    "portfolio_id, symbol, is_ros, is_paid_ros, volume, value, total_cost, total_cost_sep FROM "
                    "[nooredenadb].[company].[trades] UNION ALL SELECT date, type, portfolio_id, symbol, is_ros, "
                    f"is_paid_ros, volume, value, total_cost, total_cost_sep FROM [nooredenadb].[brokers].[trades_last]"
                    f" WHERE date > (SELECT MAX(date) as date FROM [nooredenadb].[company].[trades])) TEMP "
                    f"WHERE type = {trade_type} AND date >= '{start_date}' AND date <= '{end_date}' "
                    f"GROUP BY portfolio_id, symbol, is_ros, is_paid_ros")
    trades = pd.read_sql(query_trades, db_conn)
    trades = trades[~trades["symbol"].str[:1].isin(["ض", "ط"])].reset_index(drop=True)

    if fix_exclude:
        trades = trades[~trades["symbol"].isin(get_fix_income_symbols()["symbol"])].reset_index(drop=True)

    if main_portfolio:
        if not prx_portfolio:
            trades = trades[trades["portfolio_id"] == 1].reset_index(drop=True)
        trades.drop(labels=["total_cost_sep"], axis=1, inplace=True)

    if (not main_portfolio) and prx_portfolio:
        trades = trades[trades["portfolio_id"] != 1].reset_index(drop=True)
        trades.drop(labels=["total_cost"], axis=1, inplace=True)
        trades.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    trades["basket"] = ["اصلی" if trades["portfolio_id"].iloc[i] == 1 else "prx" for i in range(len(trades))]
    trades.sort_values(by="value", ascending=False, ignore_index=True, inplace=True)
    trades["mean_price"] = round(trades["value"] / trades["volume"]).astype(int)
    trades["share_percent"] = trades["value"] / trades["value"].sum()
    trades = trades[cols + (trade_type - 1) * ["total_cost"]]
    symbols = get_symbols_data()[["symbol", "sector_name"]]
    symbols_ros = symbols.copy()
    symbols_ros["symbol"] = symbols_ros["symbol"] + "ح"
    symbols = pd.concat([symbols, symbols_ros], axis=0, ignore_index=True)
    trades = trades.merge(symbols, on="symbol", how="left")
    return trades


def pie_chart_data(trades_df: pd.DataFrame, agg_percent: float = 0.01) -> pd.DataFrame:
    trades_df = trades_df[["symbol", "sector_name", "volume", "value", "share_percent"]].groupby(
        by=["symbol", "sector_name"], as_index=False).sum()
    trades_df["mean_price"] = round(trades_df["value"] / trades_df["volume"]).astype(int)
    trades_pie_small = trades_df[trades_df["share_percent"] < agg_percent]
    if len(trades_pie_small) > 0:
        trades_small_total = pd.DataFrame([{
            "symbol": "سایر", "volume": trades_pie_small["volume"].sum(), "value": trades_pie_small["value"].sum(),
            "share_percent": trades_pie_small["share_percent"].sum()}])
        pie_chart_df = pd.concat(
            [trades_df[trades_df["share_percent"] >= 0.01], trades_small_total], axis=0, ignore_index=True)
    else:
        pie_chart_df = trades_df
    return pie_chart_df


def get_trades_buy(start_date: str, end_date: str, fix_exclude:bool = True,
                   main_portfolio: bool = True, prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:
    buy = get_trades(trade_type=1, start_date=start_date, end_date=end_date, fix_exclude=fix_exclude,
                     main_portfolio=main_portfolio, prx_portfolio=prx_portfolio)
    buy_pie = pie_chart_data(trades_df=buy, agg_percent=0.01)
    buy_total = pd.DataFrame([{"symbol": "مجموع", "value": buy["value"].sum(), "share_percent": 1}])
    return {"buy": buy, "buy_total": buy_total, "buy_pie": buy_pie}


def get_trades_sell(start_date: str, end_date: str, fix_exclude:bool = True,
                    main_portfolio: bool = True, prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:
    sell = get_trades(trade_type=2, start_date=start_date, end_date=end_date, fix_exclude=fix_exclude,
                      main_portfolio=main_portfolio, prx_portfolio=prx_portfolio)
    sell["profitloss"] = sell["value"] - sell["total_cost"]
    sell["profitloss_percent"] = sell["profitloss"] / sell["total_cost"]
    sell_pie = pie_chart_data(trades_df=sell, agg_percent=0.01)
    sell_total = pd.DataFrame([{"symbol": "مجموع", "value": sell["value"].sum(), "total_cost": sell["total_cost"].sum(),
                                "profitloss": sell["profitloss"].sum(), "share_percent": 1,
                                "profitloss_percent": sell["profitloss"].sum() / sell["total_cost"].sum()}])
    return {"sell": sell, "sell_total": sell_total, "sell_pie": sell_pie}


########################################################################################################################


def get_avalhami_history() -> pd.DataFrame:
    q_ = ("SELECT JalaliDate date, SellNAVPerShare final_price, ISNULL(TEMP.funds_unit,0) amount,ISNULL(TEMP.cost, 0) "
          "total_cost FROM [nooredenadb].[extra].[avalhami_nav] FULL JOIN (SELECT date,SUM((CASE WHEN type=1 THEN "
          "funds_unit ELSE funds_unit*-1 END)) funds_unit,SUM((CASE WHEN type=1 THEN value ELSE cost * -1 END)) AS cost"
          " FROM [nooredenadb].[extra].[avalhami_trades] GROUP BY date) TEMP ON avalhami_nav.JalaliDate=TEMP.date")
    avalhami_history = pd.read_sql(q_, db_conn)
    avalhami_history.sort_values("date", inplace=True, ignore_index=True)
    avalhami_history["volume"] = avalhami_history["amount"].cumsum()
    avalhami_history["total_cost"] = avalhami_history["total_cost"].cumsum()
    avalhami_history["total_cost_sep"] = avalhami_history["total_cost"]
    avalhami_history["value"] = avalhami_history["volume"] * avalhami_history["final_price"]
    avalhami_history = avalhami_history[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()
    return avalhami_history

def get_avalhami_profitloss() -> pd.DataFrame:
    q_ = "SELECT date, (value - cost) profitloss FROM [nooredenadb].[extra].[avalhami_trades] WHERE type=2"
    avalhami_profitloss = pd.read_sql(q_, db_conn)
    avalhami_profitloss["date_"] = avalhami_profitloss["date"].str[:7]
    avalhami_profitloss = avalhami_profitloss[["date_", "profitloss"]].groupby(by="date_", as_index=False).sum()
    return avalhami_profitloss

def get_portfolio_history(start_date: str, end_date: str, include_avalhami: bool = True,
                          main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    if (not main_portfolio) and include_avalhami:
        raise ValueError("both main and avalham could not be False!!! main bust be True for (include_avalhami=True).")

    q_ = "SELECT * FROM [nooredenadb].[company].[portfolio_history] WHERE date>='{}' AND date<='{}'"
    portfolio_history = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        portfolio_history.drop(labels=["total_cost_sep"], axis=1, inplace=True)
        if not prx_portfolio:
            portfolio_history = portfolio_history[portfolio_history["portfolio_id"] == 1].reset_index(drop=True)

    if (not main_portfolio) and prx_portfolio:
        portfolio_history = portfolio_history[portfolio_history["portfolio_id"] != 1].reset_index(drop=True)
        portfolio_history.drop(labels=["total_cost"], axis=1, inplace=True)
        portfolio_history.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    portfolio_history["value"] = portfolio_history["volume"] * portfolio_history["final_price"]
    portfolio_history = portfolio_history[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()

    if main_portfolio and include_avalhami:
        avalhami_df = get_avalhami_history()
        avalhami_df.rename(columns={"total_cost": "total_cost_avalhami", "value": "value_avalhami"}, inplace=True)
        portfolio_history = portfolio_history.merge(avalhami_df, on="date", how="left")
        portfolio_history["value_avalhami"].ffill(inplace=True)
        portfolio_history["total_cost_avalhami"].ffill(inplace=True)
        portfolio_history["value"] += portfolio_history["value_avalhami"]
        portfolio_history["total_cost"] += portfolio_history["total_cost_avalhami"]
        portfolio_history = portfolio_history[["date", "value", "total_cost"]]
    else:
        pass
    return portfolio_history

def get_dividend(start_date: str, end_date: str, main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    q_ = ("SELECT date, dividend, portfolio_id FROM [nooredenadb].[portfolio].[portfolio_dividend]"
          " WHERE date>'{}' AND date<='{}'")
    dividend = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        if not prx_portfolio:
            dividend = dividend[dividend["portfolio_id"] == 1].reset_index(drop=True, inplace=False)
    if (not main_portfolio) and prx_portfolio:
        dividend = dividend[dividend["portfolio_id"] != 1].reset_index(drop=True, inplace=False)

    dividend["date_"] = dividend["date"].str[:7]
    dividend = dividend[["date_", "dividend"]].groupby(by=["date_"], as_index=False).sum()
    return dividend

def get_profitloss(start_date: str, end_date: str, include_avalhami: bool = True,
                   main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    if (not main_portfolio) and include_avalhami:
        raise ValueError("both main and avalham could not be False!!! main bust be True for (include_avalhami=True).")

    q_ = ("SELECT date, portfolio_id, value, total_cost, total_cost_sep FROM (SELECT date, portfolio_id, type, "
          "value, total_cost, total_cost_sep FROM [nooredenadb].[company].[trades] UNION SELECT date, portfolio_id,"
          " type, value, total_cost, total_cost_sep FROM [nooredenadb].[brokers].[trades_last] WHERE date > (SELECT"
          " MAX(date) FROM [nooredenadb].[company].[trades]) AND SUBSTRING(symbol, 1, 1) NOt IN ('ض', 'ط')) AS TEMP"
          " WHERE type=2 AND date>'{}' AND date<='{}'")
    profitloss = pd.read_sql(q_.format(start_date, end_date), db_conn)

    if main_portfolio:
        if not prx_portfolio:
            profitloss = profitloss[profitloss["portfolio_id"] == 1].reset_index(drop=True, inplace=False)
        profitloss.drop(labels=["total_cost_sep"], axis=1, inplace=True)

    if (not main_portfolio) and prx_portfolio:
        profitloss = profitloss[profitloss["portfolio_id"] != 1].reset_index(drop=True, inplace=False)
        profitloss.drop(labels=["total_cost"], axis=1, inplace=True)
        profitloss.rename({"total_cost_sep": "total_cost"}, axis=1, inplace=True)

    profitloss["profitloss"] = profitloss["value"] - profitloss["total_cost"]
    profitloss["date_"] = profitloss["date"].str[:7]
    profitloss = profitloss[["date_", "profitloss"]].groupby(by=["date_"], as_index=False).sum()

    if main_portfolio and include_avalhami:
        avalhami_profitloss = get_avalhami_profitloss()
        avalhami_profitloss.rename(columns={"profitloss": "profitloss_avalhami"}, inplace=True)
        profitloss = profitloss.merge(avalhami_profitloss, on="date_", how="left")
        profitloss["profitloss"] += profitloss["profitloss_avalhami"].fillna(0)
        profitloss = profitloss[["date_", "profitloss"]]
    else:
        pass
    return profitloss

def get_index(index_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    q_ = (f"SELECT TEMP1.close_price,TEMP2.date FROM (SELECT TRY_CONVERT(INT,REPLACE(Miladi,'-','')) date_m,"
          f"Jalali_1 date FROM [nooredenadb].[extra].[dim_date]) TEMP2 LEFT JOIN (SELECT * FROM "
          f"[nooredenadb].[tsetmc].[indices_history] WHERE indices_id='{index_id}') TEMP1 "
          f"ON TEMP1.date=TEMP2.date_m ORDER BY TEMP2.date")
    index_df = pd.read_sql(q_, db_conn)
    index_df["close_price"].ffill(inplace=True)
    index_df["date_"] = index_df["date"].str[:7]
    index_df.drop_duplicates(subset="date_", keep="last", inplace=True, ignore_index=True)
    index_df = index_df[(index_df["date"] >= start_date) & (index_df["date"] <= end_date)][["date_", "close_price"]]
    return index_df

def get_indices(start_date: str, end_date: str) -> pd.DataFrame:
    index_names_mapper = {"100": "total_index", "104": "price_index_eq", "56": "investment_index"}
    q_ = "SELECT indices,indices_name,indices_id FROM [nooredenadb].[tsetmc].[indices] WHERE indices IN ('100','104','56')"
    indices = pd.read_sql(q_, db_conn)
    indices.sort_values(by="indices_name", ascending=False, ignore_index=True, inplace=True)
    indices_df = pd.DataFrame(columns=["date_"])
    for idx_ in range(len(indices)):
        index_name = index_names_mapper[indices["indices"].iloc[idx_]]
        tmp = get_index(indices["indices_id"].iloc[idx_], start_date, end_date)
        q__ = (f"SELECT SUBSTRING(REPLACE(date,'-','/'), 1, 7) date_,close_price FROM "
               f"[nooredenadb].[tsetmc].[indices_data_today] WHERE indices_id='{indices["indices_id"].iloc[idx_]}'")
        tmp_today = pd.read_sql(q__, db_conn)
        tmp = pd.concat([tmp, tmp_today], axis=0, ignore_index=True)
        tmp.drop_duplicates(subset="date_", keep="last", inplace=True, ignore_index=True)
        tmp["return"] = tmp["close_price"].diff(periods=1) / tmp["close_price"].shift(periods=1)
        tmp.rename({"close_price": index_name, "return": "return_" + index_name}, axis=1, inplace=True)
        indices_df = indices_df.merge(tmp, on="date_", how="outer")
    return indices_df

def get_bazdehi_table(start_date: str, end_date: str, include_avalhami: bool = True,
                      main_portfolio: bool = True, prx_portfolio: bool = True) -> pd.DataFrame:
    if start_date >= end_date:
        raise ValueError("start_date must be less than end_date")
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    if (not main_portfolio) and include_avalhami:
        raise ValueError("both main and avalham could not be False!!! main bust be True for (include_avalhami=True).")

    portfolio_ = get_portfolio(date="last", main_portfolio=main_portfolio,
                               prx_portfolio=prx_portfolio, include_avalhami=include_avalhami)
    portfolio_df = get_portfolio_history(start_date, end_date, main_portfolio=main_portfolio,
                                         prx_portfolio=prx_portfolio, include_avalhami=include_avalhami)

    portfolio_df = pd.concat([
        portfolio_df,pd.DataFrame([{"date": portfolio_["date"].iloc[0],"value": portfolio_["value"].sum(),
                                    "total_cost": portfolio_["total_cost"].sum()}])], axis=0, ignore_index=True)
    if (not main_portfolio) and prx_portfolio:
        if start_date < portfolio_df["date"].min():
            portfolio_df = pd.concat(
                [portfolio_df,
                 pd.DataFrame([{"date": "1404/01/31", "value": 445315103610, "total_cost": 445315103610}])], axis=0)
            portfolio_df.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)

    portfolio_df = portfolio_df[portfolio_df["date"] <= end_date].reset_index(drop=True, inplace=False)
    portfolio_df["date_"] = portfolio_df["date"].str[:7]

    dividend_df = get_dividend(start_date, end_date, main_portfolio=main_portfolio, prx_portfolio=prx_portfolio)
    profitloss_df = get_profitloss(start_date, end_date, main_portfolio=main_portfolio,
                                   prx_portfolio=prx_portfolio, include_avalhami=include_avalhami)

    bazdehi_df = portfolio_df.merge(dividend_df, on="date_", how="left").merge(profitloss_df, on="date_", how="left")

    bazdehi_df["return_portfolio"] = ((((bazdehi_df["dividend"].fillna(0) + bazdehi_df["profitloss"].fillna(0)) +
                                        (bazdehi_df["value"] - bazdehi_df["total_cost"])) - (
            bazdehi_df["value"].shift(periods=1) - bazdehi_df["total_cost"].shift(periods=1))) /
                                      bazdehi_df["value"].shift(periods=1))
    indices_df = get_indices(max(start_date, portfolio_df["date"].min()), end_date)
    bazdehi_df = bazdehi_df.merge(indices_df, on="date_", how="left")
    bazdehi_df.drop(labels=["date_"], axis=1, inplace=True)

    bazdehi_table_retuns = bazdehi_df[
        ["date", "return_portfolio", "return_total_index","return_price_index_eq", "return_investment_index"]]
    df2 = bazdehi_table_retuns.dropna(subset=['return_portfolio']).reset_index(drop=True)
    bazdehi_table_retuns_3_month = (df2.groupby(df2.index // 3, sort=True).agg(
        date=('date', 'last'),
        return_portfolio_3=("return_portfolio", lambda s: (1 + s).prod() - 1),
        return_total_index_3=("return_total_index", lambda s: (1 + s).prod() - 1),
        return_price_index_eq_3=("return_price_index_eq", lambda s: (1 + s).prod() - 1),
        return_investment_index_3=("return_investment_index", lambda s: (1 + s).prod() - 1)))
    bazdehi_df = bazdehi_df.merge(bazdehi_table_retuns_3_month, on=["date"], how="left")
    return bazdehi_df


########################################################################################################################

def date_dropdown_provider(get_last: bool = True) -> dict[str, Union[str, list[dict[str, str]]]]:
    dates_ = pd.read_sql("SELECT DISTINCT(date) value FROM [nooredenadb].[company].[portfolio_history] ORDER BY date DESC", db_conn)
    dates_["label"] = dates_["value"]
    prev_12 = dates_["value"].iloc[12]
    if get_last:
        dates_ = pd.concat([pd.DataFrame([{"value": "last", "label": "آخرین وضعیت"}]), dates_], axis=0, ignore_index=True)
    dates_ = dates_.to_dict(orient="records")
    return {"options": dates_, "12prev": prev_12}




########################################################################################################################


if __name__ == "__main__":

    today = jdatetime.datetime.today()

    report_date = today.strftime('%Y-%m-%d')

    buy_sell_start_date = "1404/07/09"
    buy_sell_end_date = today.strftime('%Y/%m/%d')

    bazdehi_start_date_annually = "1403/06/31"
    bazdehi_start_date_fiscal_year = "1403/10/30"
    bazdehi_end_date_annually = bazdehi_end_date_fiscal_year = today.strftime('%Y/%m/%d')

    portfolio_test = get_portfolio()

    buy = get_trades_buy(start_date=buy_sell_start_date, end_date=buy_sell_end_date)
    buy, buy_total, buy_pie = buy["buy"], buy["buy_total"], buy["buy_pie"]

    sell = get_trades_sell(start_date=buy_sell_start_date, end_date=buy_sell_end_date)
    sell, sell_total, sell_pie = sell["sell"], sell["sell_total"], sell["sell_pie"]


    # "annual" RETURN of portfolio
    # "with" avalhami Fund
    bazdehi_table_annually_avalhami = get_bazdehi_table(
        start_date=bazdehi_start_date_annually, end_date=bazdehi_end_date_annually, include_avalhami=True)
    bazdehi_table_annually_avalhami.to_excel("c:/users/h.damavandi/desktop/annually_avalhami.xlsx", index=False)
    # "without" avalhami Fund
    bazdehi_table_annually = get_bazdehi_table(
        start_date=bazdehi_start_date_annually, end_date=bazdehi_end_date_annually, include_avalhami=False)
    bazdehi_table_annually.to_excel("c:/users/h.damavandi/desktop/annually.xlsx", index=False)


    # "fiscal" RETURN of portfolio with avalhami Fund
    # "with" avalhami Fund
    bazdehi_table_fiscal_year_avalhami = get_bazdehi_table(
        start_date=bazdehi_start_date_fiscal_year, end_date=bazdehi_end_date_fiscal_year, include_avalhami=True)
    bazdehi_table_fiscal_year_avalhami.to_excel("c:/users/h.damavandi/desktop/fiscal_year_avalhami.xlsx", index=False)
    # "without" avalhami Fund
    bazdehi_table_fiscal_year = get_bazdehi_table(
        start_date=bazdehi_start_date_fiscal_year, end_date=bazdehi_end_date_fiscal_year, include_avalhami=False)
    bazdehi_table_fiscal_year.to_excel("c:/users/h.damavandi/desktop/fiscal_year.xlsx", index=False)


