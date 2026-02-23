import pandas as pd
import warnings, jdatetime
from typing import Literal, Optional

from utils import database


warnings.filterwarnings("ignore")
db_conn = database.make_connection()
sectors_mapper = {
    'ماشین آلات و تجهیزات': "ماشین آلات", 'بانکها و موسسات اعتباری': "بانک ها", 'چندرشته ای صنعتی': "هلدینگ",
    'خودرو و قطعات': "خودرو", 'سرمایه گذاریها': "سرمایه گذاری ها", 'استخراج زغال سنگ': "زغال سنگ",
    'سیمان آهک گچ': "سیمان", 'استخراج کانه های فلزی': "معادن", 'صندوق سرمایه گذاری قابل معامله': "صندوق",
    'فرآورده های نفتی': "پالایشی", 'بیمه و بازنشستگی': "بیمه", 'کاشی و سرامیک': "کاشی", 'فلزات اساسی': "فلزات",
    'لاستیک و پلاستیک': "لاستیک", 'عرضه برق،گاز،بخار و آب گرم': "یوتیلیتی", 'زراعت و خدمات وابسته': "زراعی",
    'قند و شکر': "قندی", 'غذایی بجز قند وشکر': "غذایی", 'حمل و نقل انبارداری و ارتباطات ': "حمل و نقل",
    # 'فنی و مهندسی': "", 'دارویی': "", 'ابزار پزشکی': "", 'شیمیایی': "", 'مخابرات': "",
    # 'خرده فروشی': "", 'دستگاههای برقی': "", 'رایانه': "", 'اطلاعات و ارتباطات': "",
}

def get_weekends_jalali():
    weekends_jalali = pd.read_sql("SELECT Jalali_1 date FROM [nooredenadb].[extra].[dim_date] "
                                  "WHERE MWeekDay IN ('Friday', 'Thursday')", db_conn)
    return weekends_jalali



def get_portfolio_specs() -> pd.DataFrame:
    portfolio_specs = pd.read_sql("SELECT * FROM [nooredenadb].[brokers].[portfolio_specifics]", db_conn)
    return portfolio_specs

def check_basket(l: set):
    p_s = get_portfolio_specs()
    prx = set(p_s[p_s["portfolio_type"] != "main"]["portfolio_id"].values.tolist())
    check_1, check_2 =  1 in l, bool(l & prx)
    if check_1 and check_2: return "هر دو"
    elif check_1: return "اصلی"
    elif check_2: return "prx"
    else: return "نامشخص"

def get_fix_income_symbols() -> pd.DataFrame:
    query_fix_income_symbols = "SELECT [symbol] FROM [nooredenadb].[tsetmc].[symbols] WHERE subsector='6812'"
    fix_income_symbols = pd.read_sql(query_fix_income_symbols, db_conn)
    return fix_income_symbols

########################################################################################################################


def get_trades_value(start_date: str, end_date: str, trade_type: Optional[Literal[1, 2]] = None,
                     include_options: bool = False, include_fix_funds: bool = False, main_portfolio: bool = True,
                     prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:

    if not((trade_type in [1, 2]) or (trade_type is None)): raise ValueError("trade_type must be either 1 or 2 or None")
    if (not main_portfolio) and (not prx_portfolio): raise ValueError("both main and prx could not be False!")

    query_trades = ("SELECT date, portfolio_id, symbol, type, SUM(value) value FROM [nooredenadb].[brokers].[trades] "
                    f"WHERE date >= '{start_date}' AND date <= '{end_date}' GROUP BY date, portfolio_id, symbol, type")
    trades = pd.read_sql(query_trades, db_conn)
    if not include_options:
        trades = trades[~trades["symbol"].str[:1].isin(["ض", "ط"])]
    if not include_fix_funds:
        trades = trades[~trades["symbol"].isin(get_fix_income_symbols()["symbol"])]
    if main_portfolio and (not prx_portfolio):
        trades = trades[trades["portfolio_id"] == 1]
    if (not main_portfolio) and prx_portfolio:
        trades = trades[trades["portfolio_id"] != 1]
    if trade_type is not None:
        trades = trades[trades["type"] == trade_type]
    trades = trades[["date", "value"]].groupby(by=["date"], as_index=False).sum()
    trades.sort_values(by="date", ascending=True, inplace=True, ignore_index=True)
    return trades

##################################################

def get_credits(start_date: str, end_date: str, main_portfolio: bool = True,
                prx_portfolio: bool = True) -> dict[str: pd.DataFrame]:

    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    start_date_m = jdatetime.datetime.strptime(start_date, "%Y/%m/%d").togregorian().strftime("%Y-%m-%d")
    credits_rayan = pd.read_sql("SELECT tr.transactionDate AS [date], tr.remaining, tr.broker_id, tr.portfolio_id FROM"
                                " nooredenadb.brokers.trades_rayan AS tr JOIN (SELECT MAX(row_) AS row_ FROM "
                                f"[nooredenadb].[brokers].[trades_rayan] WHERE transactionDate >= '{start_date}' "
                                "GROUP BY transactionDate, broker_id, portfolio_id ) AS last_rows "
                                "ON tr.row_ = last_rows.row_ ORDER BY tr.transactionDate, tr.portfolio_id, "
                                "tr.broker_id;", db_conn)
    credits_tadbir = pd.read_sql("WITH last_rows AS (SELECT MAX(row_) AS row_ FROM "
                                 f"[nooredenadb].[brokers].[trades_tadbir_ledger] WHERE "
                                 f"TransactionDate >= '{start_date_m}' AND Description NOT "
                                 f"IN (N'سند افتتاحیه مورخ {0}', N'سند اختتامیه') GROUP BY "
                                 "substring(TransactionDate, 1, 10), broker_id, portfolio_id), d AS (SELECT Miladi,"
                                 f" Jalali_1 FROM [nooredenadb].[extra].[dim_date] WHERE Jalali_1 >= '{start_date}' AND"
                                 " Miladi <= CAST(GETDATE() AS date)) SELECT tr.broker_id, tr.portfolio_id, tr.Remain "
                                 "AS remaining, d.Jalali_1 AS [date] FROM [nooredenadb].[brokers].[trades_tadbir_ledger]"
                                 " AS tr JOIN last_rows lr ON tr.row_ = lr.row_ JOIN d ON substring(TransactionDate,"
                                 " 1, 10) = d.Miladi ORDER BY d.Jalali_1, tr.portfolio_id, tr.broker_id;", db_conn)
    credits = pd.concat([credits_rayan, credits_tadbir], axis=0, ignore_index=True)

    credits_index = pd.MultiIndex.from_product(
        [credits["date"].unique(), credits["broker_id"].unique(), credits["portfolio_id"].unique()],
        names=['date', 'broker_id', "portfolio_id"])
    credits_df = pd.DataFrame(index=credits_index).reset_index(drop=False, inplace=False)
    credits_df = credits_df.merge(credits, on=["date", "broker_id", "portfolio_id"], how="left").sort_values(
        by=["portfolio_id", "broker_id", "date"], inplace=False)

    credits_df["remaining"] = credits_df.groupby(["portfolio_id", "broker_id"])["remaining"].ffill()
    credits_df["remaining"] = (credits_df["remaining"] < 0) * credits_df["remaining"]

    if main_portfolio and (not prx_portfolio):
        credits_df = credits_df[credits_df["portfolio_id"] == 1]
    if (not main_portfolio) and prx_portfolio:
        credits_df = credits_df[credits_df["portfolio_id"] != 1]

    credits_df = credits_df[["date", "remaining"]].groupby(by="date", as_index=False).sum()
    credits_df = credits_df[credits_df["date"] <= end_date].sort_values(by="date", ascending=True, ignore_index=True)
    credits_df["remaining"] *= -1
    return credits_df

##################################################

def get_portfolio_data_daily(start_date: str, end_date: str, main_portfolio: bool = True,
                             prx_portfolio: bool = True) -> pd.DataFrame:

    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")

    sigma_portfolio = pd.read_sql("SELECT portfolio_id, date, sum(total_cost) cost, sum(gross_value_final_price) value"
                                  f" FROM [nooredenadb].[sigma].[portfolio] where date >= '{start_date}' and "
                                  f"date <= '{end_date}' and type in ('صندوق', 'گواهی سپرده کالایی', 'حق تقدم', 'سهام') "
                                  "group by portfolio_id, date", db_conn)
    sigma_dividend = pd.read_sql("SELECT portfolio_id, meeting_date date, sum(value) dividend FROM "
                                 f"[nooredenadb].[sigma].[dividend] where meeting_date >= '{start_date}' and "
                                 f"meeting_date <= '{end_date}' and value > 0 group by portfolio_id, meeting_date",
                                 db_conn)
    sigma_profit = pd.read_sql("SELECT portfolio_id, date, sum(net_profit) profit FROM [nooredenadb].[sigma].[buysell]"
                               f" where date >= '{start_date}' and date <= '{end_date}' and action='فروش' and "
                               "type!='اختیار معامله' group by portfolio_id, date", db_conn)
    if main_portfolio and (not prx_portfolio):
        sigma_portfolio = sigma_portfolio[sigma_portfolio["portfolio_id"] == 1]
        sigma_dividend = sigma_dividend[sigma_dividend["portfolio_id"] == 1]
        sigma_profit = sigma_profit[sigma_profit["portfolio_id"] == 1]
    if (not main_portfolio) and prx_portfolio:
        sigma_portfolio = sigma_portfolio[sigma_portfolio["portfolio_id"] != 1]
        sigma_dividend = sigma_dividend[sigma_dividend["portfolio_id"] != 1]
        sigma_profit = sigma_profit[sigma_profit["portfolio_id"] != 1]

    sigma_portfolio = sigma_portfolio.drop(
        labels="portfolio_id",axis=1, inplace=False).groupby("date", as_index=False).sum()
    sigma_dividend = sigma_dividend.drop(
        labels="portfolio_id",axis=1, inplace=False).groupby("date", as_index=False).sum()
    sigma_profit = sigma_profit.drop(
        labels="portfolio_id",axis=1, inplace=False).groupby("date", as_index=False).sum()
    portfolio = sigma_portfolio.merge(
        sigma_profit, on="date", how="outer").merge(sigma_dividend, on="date", how="outer").fillna(0, inplace=False)

    return portfolio

##################################################

def get_sectors_value_daily(start_date: str, end_date: str, main_portfolio: bool = True, prx_portfolio: bool = True,
                            drop_weekends: bool = True, convert_to_br: bool = True) -> pd.DataFrame:
    if (not main_portfolio) and (not prx_portfolio):
        raise ValueError("both main and prx could not be False!")
    query_sector_value = (f"SELECT portfolio_id, date, SUM(value) AS value, sector FROM (SELECT portfolio_id, date, "
                          f"gross_value_final_price value, CASE WHEN symbol = 'دارایکم' THEN 'بانکها و موسسات اعتباری' "
                          f"WHEN symbol = 'گنگین' THEN 'چندرشته ای صنعتی' ELSE sector END sector FROM "
                          f"[nooredenadb].[sigma].[portfolio] WHERE date >= '{start_date}' AND date <= '{end_date}')"
                          f" TEMP GROUP BY portfolio_id, date, sector ORDER BY date")
    sector_value = pd.read_sql(query_sector_value, db_conn)
    if main_portfolio and (not prx_portfolio):
        sector_value = sector_value[sector_value["portfolio_di"] ==1]
    if (not main_portfolio) and prx_portfolio:
        sector_value = sector_value[sector_value["portfolio_di"] != 1]
    if drop_weekends:
        weekends = get_weekends_jalali()
        weekends["is_weekend"] = True
        sector_value = sector_value.merge(weekends, on="date", how="left").fillna({"is_weekend": False}, inplace=False)
        sector_value = sector_value[~sector_value["is_weekend"]].sort_values(by=["date", "sector"], ignore_index=True)
    sector_value = sector_value.drop(labels=["portfolio_id", "is_weekend"], axis=1, inplace=False).groupby(
        by=["date", "sector"], as_index=False).sum()
    sector_value["sector"].replace(sectors_mapper, inplace=True, regex=False)
    if convert_to_br:
        sector_value["value"] = (sector_value["value"] / 1e9).round(0)
    sector_value_new_format = pd.pivot_table(sector_value, index="date", columns="sector", values="value", fill_value=0)
    # sector_value_new_format = sector_value_new_format.reset_index(drop=False, names=["date"], inplace=False)
    return sector_value_new_format

