import warnings
import jdatetime
import numpy as np
import pandas as pd

from utils.database import make_connection
from trade_intel.utils.funcs import get_last_date, get_next_date, capital_increase, CapChangeParams


start_day = "1403/10/30"
end_day = "1404/11/01"
warnings.filterwarnings("ignore")
one_day = jdatetime.timedelta(days=1)
db_conn = make_connection()
# fixed_income_id = "3846143218462419"
# fixed_income_name = "افران"
credit_APR = 0.28
credit = 3_316_277_911_780  # 1403/10/30
closed_days = pd.read_sql("SELECT REPLACE([date], '-', '/') as date FROM [nooredenadb].[extra].[closed_days]",
                          db_conn)

################################################################################################

dps_funds = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps_funds]", db_conn)
dps_funds["date"] = dps_funds.apply(lambda row: jdatetime.datetime.fromgregorian(
    year=int(row["date"][:4]), month=int(row["date"][5:7]), day=int(row["date"][8:10])
).strftime("%Y/%m/%d") if row["date"] is not None else None, axis=1)

dps = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps]", db_conn)
for c in ['fiscal_year', 'date_time', 'announcement_date']:
    dps[c] = dps.apply(lambda row: jdatetime.datetime.fromgregorian(
        year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
    ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

capital_changes = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[capital_changes]", db_conn)
for c in ['date', 'underwriting_end_date', 'registration_date', 'stock_certificate_receive_date', 'warrant_sell_date']:
    capital_changes[c] = capital_changes.apply(lambda row: jdatetime.datetime.fromgregorian(
        year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
    ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

################################################################################################

query_cashflow = (f"SELECT * FROM [nooredenadb].[company].[cashflow] where date > '{start_day}' "
                  f"and (meeting_date <= '{start_day}' or debtor > 0) order by date")
cashflow = pd.read_sql(query_cashflow, db_conn)

# query_avalhami_raw = ("SELECT [JalaliDate] date, [SellNAVPerShare] final_price, ISNULL(TEMP.funds_unit, 0) amount, "
#                       "ISNULL(TEMP.cost, 0) total_cost FROM [nooredenadb].[extra].[avalhami_nav] LEFT JOIN (SELECT date,"
#                       " SUM((CASE WHEN type=1 THEN funds_unit ELSE funds_unit * -1 END)) funds_unit, SUM((CASE WHEN "
#                       "type=1 THEN value ELSE cost * -1 END)) AS cost FROM [nooredenadb].[extra].[avalhami_trades] "
#                       "GROUP BY date) AS TEMP ON avalhami_nav.JalaliDate=TEMP.date")
# avalhami_df = pd.read_sql(query_avalhami_raw, db_conn)
# avalhami_df = avalhami_df[(avalhami_df["date"] > start_day) & (avalhami_df["amount"] < 0)]
# avalhami_df["creditor"] = avalhami_df["amount"] * avalhami_df["final_price"] * -1
# avalhami_df = avalhami_df[["date", "creditor"]]
# avalhami_df["debtor"] = 0
# avalhami_df["source"] = "حامی اول"
# avalhami_df["description"] = "واریز به حساب بابت ابطال واحدهای صندوق بازارگردانی حامی اول (حامی اول)"
#
# cashflow = pd.concat([cashflow, avalhami_df], axis=0, ignore_index=True)

cashflow["due_date"] = [get_last_date(date_=cashflow["date"].iloc[i]) if ~cashflow["date"].isna().iloc[i] else np.nan
                        for i in range(len(cashflow))]
cashflow["paid"] = False

put_options = pd.read_sql("SELECT * FROM [nooredenadb].[sigma].[put_option]", db_conn)
put_options["payday"] = [get_next_date(date_=put_options["maturity_date"].iloc[i]) for i in range(len(put_options))]
put_options["done"] = False

rahavard_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[symbols]", db_conn)
portfolio = pd.read_sql("SELECT [symbol], [type], [status], [amount], [total_cost], [final_price], [date] "
                        "FROM [nooredenadb].[sigma].[portfolio] WHERE date = (SELECT MAX(date) FROM "
                        f"[nooredenadb].[sigma].[portfolio] WHERE date <= '{start_day}')", db_conn)
portfolio["type"].replace({"اختیار معامله | موقعیت فروش": "put_option", "اختیار معامله | موقعیت خرید": "call_option",
                           "صندوق": "fund", "سلف موازی": "forward_contract", "گواهی سپرده کالایی": "commodity_cd",
                           "حق تقدم": "ros", "اوراق": "bond", "سهام": "stock"}, inplace=True, regex=False)
portfolio["status"].replace({r"حق\u200cتقدم پذیره نویسی شده": "paid_ros", "سهام جایزه": "bonus_share",
                             "حق تقدم عادی": "normal_ros", "حق تقدم استفاده نشده": "unused_ros",
                             "سهام عادی": "normal_share", "عادی": "normal_share"}, inplace=True, regex=False)
portfolio["status"].iloc[portfolio.index[portfolio["symbol"] == "آکنتور"].values[0]] = "bonus_share"
portfolio = pd.concat([portfolio, pd.get_dummies(portfolio["type"]), pd.get_dummies(portfolio["status"])],
                       axis=1).drop(columns=["type", "status"], inplace=False)
for c in ["put_option", "call_option", "fund", "ros", "stock", "paid_ros", "bonus_share", "normal_ros", "normal_share"]:
    if c not in portfolio.columns.values.tolist():
        portfolio[c] = False
portfolio = portfolio[~portfolio["call_option"] & ~portfolio["put_option"]].drop(columns=["call_option", "put_option"])
portfolio['original_symbol'] = portfolio.apply(
    lambda row: row['symbol'][:len(row["symbol"]) + (row["normal_ros"] * -1)], axis=1)
portfolio = portfolio.merge(rahavard_symbols[["symbol", "asset_id"]].rename(
    {"symbol": "original_symbol"}, axis=1, inplace=False), on="original_symbol", how="left")
portfolio_value_start = int((portfolio["amount"] * portfolio["final_price"]).sum())
symbols = pd.read_sql("SELECT [symbol] as tse_symbol, [symbol_id] FROM [nooredenadb].[tsetmc].[symbols]",
                      db_conn)
symbols_ros = pd.read_sql("SELECT [symbol_ros] as tse_symbol , [symbol_id] FROM [nooredenadb].[tsetmc].[symbols_ros]",
                          db_conn)
portfolio["tse_symbol"] = portfolio["symbol"].replace(
    {"دارایکم": "دارا يكم"},regex=False, inplace=False).replace(
    {"ی": "ي", "ک": "ك"}, regex=True, inplace=False)

################################################################################################

portfolio["ros_payday"] = [np.nan] * len(portfolio)
portfolio["registration_date"] = [np.nan] * len(portfolio)
for i in range(len(portfolio)):
    if ~portfolio["normal_share"].iloc[i]:
        temp = capital_changes[capital_changes["asset_id"] == portfolio["asset_id"].iloc[i]]
        temp = temp[temp["date"] <= start_day]
        temp = temp[~temp["registration_date"].isnull()].sort_values("date", ascending=False, inplace=False)
        portfolio["registration_date"].iloc[i] = temp["registration_date"].iloc[0]
        if portfolio["normal_ros"].iloc[i]:
            portfolio["ros_payday"].iloc[i] = temp["underwriting_end_date"].iloc[0]
portfolio["ros_due_date"] = [get_last_date(date_=portfolio["ros_payday"].iloc[i]) if
                             ~portfolio["ros_payday"].isna().iloc[i] else np.nan
                             for i in range(len(portfolio))]

credit_table = pd.DataFrame([{"date": start_day, "remaining": credit}])
dps_table = pd.DataFrame()
dps_funds_table = pd.DataFrame()
cash_table = pd.DataFrame([{"date": start_day, "desc": "مانده از قبل", "credit": 0, "debit": 0, "remain": 0}])
trade_table = pd.DataFrame()
portfolio_table = pd.DataFrame()
portfolio_table = pd.concat([portfolio_table, portfolio], axis=0, ignore_index=True)
today = start_day

##################################################################################
##################################################################################

while True:

    today = jdatetime.datetime.strptime(today, "%Y/%m/%d") + one_day
    today_weekday = today.weekday()
    tomorrow = today + one_day
    today_g = jdatetime.datetime.togregorian(today)

    today = today.strftime("%Y/%m/%d")
    tomorrow = tomorrow.strftime("%Y/%m/%d")
    today_g = today_g.strftime("%Y/%m/%d")
    today_g_ = int(today_g.replace("/", ""))

    if today >= end_day:
        break
    print(f"Loading portfolio {today}")

    credit = np.ceil(credit * (1 + (credit_APR / 365)))
    credit_table = pd.concat([credit_table, pd.DataFrame([{"date": today, "remaining": credit}])],
                             axis=0, ignore_index=True)

    ##################################################################################

    # adding stocks dps payment to cashtable

    for i in range(len(dps_table)):
        if (dps_table["paid"].iloc[i] == 0) and (dps_table["due_date"].iloc[i] == today):
            dps_table["paid"].iloc[i] = 1
            cash_tmp = pd.DataFrame([{"date": today, "desc": f"واریز سود نقدی سهم {dps_table['symbol'].iloc[i]}",
                                      "credit": dps_table["amount"].iloc[i] * dps_table["pure_dps"].iloc[i], "debit": 0,
                                      "remain": (cash_table["remain"].iloc[-1] + (
                                              dps_table["amount"].iloc[i] * dps_table["pure_dps"].iloc[i]))}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    ##################################################################################

    # adding funds dps payment to cashtable

    for i in range(len(dps_funds_table)):
        if (dps_funds_table["paid"].iloc[i] == 0) and (dps_funds_table["due_date"].iloc[i] == today):
            dps_funds_table["paid"].iloc[i] = 1
            cash_tmp = pd.DataFrame([{"date": today, "desc": f"واریز سود نقدی سهم {dps_funds_table['symbol'].iloc[i]}",
                                      "credit": dps_funds_table["amount"].iloc[i] * dps_funds_table["dividend"].iloc[i],
                                      "debit": 0,"remain": (cash_table["remain"].iloc[-1] + (
                        dps_funds_table["amount"].iloc[i] * dps_funds_table["dividend"].iloc[i]))}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    ##################################################################################

    # paying cash-outs to HamraahAval or Digikala OR recieving dps payments from previous fiscal year

    for i in range(len(cashflow)):
        if (cashflow["date"].iloc[i] == today) and (cashflow["paid"].iloc[i] == False):
            cash_tmp = pd.DataFrame([{"date": today, "desc": cashflow["description"].iloc[i]}])
            if cashflow["debtor"].iloc[i] > 0:
                cash_tmp["credit"] = 0
                cash_tmp["debit"] = cashflow["debtor"].iloc[i]
                cash_tmp["remain"] = (cash_table["remain"].iloc[-1] - cashflow["debtor"].iloc[i])
            if cashflow["creditor"].iloc[i] > 0:
                cash_tmp["credit"] = cashflow["creditor"].iloc[i]
                cash_tmp["debit"] = 0
                cash_tmp["remain"] = (cash_table["remain"].iloc[-1] + cashflow["creditor"].iloc[i])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)
            cashflow["paid"].iloc[i] = True

    ##################################################################################

    # # checking for srtoke put options payments and adding to cash_table
    #
    # put_opt_tmp = put_options[
    #     (put_options["payday"] == today) & (put_options["done"])].reset_index(drop=True, inplace=False)
    # if len(put_opt_tmp) > 0:
    #     for i in range(len(put_opt_tmp)):
    #         sorc_sym = put_opt_tmp['source_symbol'].iloc[i]
    #         sym = put_opt_tmp['symbol'].iloc[i]
    #         amont = int(put_opt_tmp['amount'].iloc[i])
    #         maturity_date = put_opt_tmp['maturity_date'].iloc[i]
    #         strike_price = int(put_opt_tmp['strike_price'].iloc[i])
    #         pt_opt_desc = f"واریز بابت اعمال اختیار فروش تبعی {sorc_sym} ({sym}) به تعداد {amont}" \
    #                       f" با قیمت {strike_price} در تاریخ {maturity_date}"
    #         cash_tmp = pd.DataFrame([{"date": today, "desc": pt_opt_desc, "credit": (amont * strike_price), "debit": 0,
    #                                   "remain": (cash_table["remain"].iloc[-1] + (amont * strike_price))}])
    #         cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    ##################################################################################

    for i in range(len(portfolio)):

        # checking for capital_change registration date (changing paid_ros and bonus_share to normal_share)

        if portfolio["registration_date"].iloc[i] == today:
            print("registring new shares (changing paid_ros and bonus share to normal share)")
            portfolio["registration_date"].iloc[i] = np.nan
            portfolio["paid_ros"].iloc[i] = False
            portfolio["bonus_share"].iloc[i] = False
            portfolio["normal_share"].iloc[i] = True

        # checking for ros payday (changing normal_ros to paid_ros and modifying total_cost)

        if portfolio["ros_payday"].iloc[i] == today:
            print("changeing normal_ros to paid_ros")
            portfolio["ros_payday"].iloc[i] = np.nan
            portfolio["ros_due_date"].iloc[i] = np.nan
            portfolio["tse_symbol"].iloc[i] = portfolio["tse_symbol"].iloc[i][:-1]
            portfolio["paid_ros"].iloc[i] = True
            portfolio["normal_ros"].iloc[i] = False
            portfolio["stock"].iloc[i] = True
            portfolio["ros"].iloc[i] = False
            if portfolio["symbol"].iloc[i][:-1] in portfolio["symbol"].values:
                portfolio["final_price"].iloc[i] = portfolio["final_price"].iloc[
                    portfolio.index[portfolio["symbol"] == portfolio["symbol"].iloc[i][:-1]].values[0]
                ]
            else:
                portfolio["final_price"].iloc[i] += 1000
            portfolio["total_cost"].iloc[i] += (portfolio["amount"].iloc[i] * 1000)
            portfolio["symbol"].iloc[i] = portfolio["symbol"].iloc[i][:-1]
            cash_tmp = pd.DataFrame([{"date": today, "desc":
                f"پرداخت 1000 ریال بابت تبدیل حق تقدم {portfolio['symbol'].iloc[i]} به سهم", "credit": 0,
                                      "debit": (portfolio["amount"].iloc[i] * 1000), "remain": (
                        cash_table["remain"].iloc[-1] - (portfolio["amount"].iloc[i] * 1000))}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    portfolio = portfolio.groupby(
        ['symbol', 'final_price', 'date', 'fund', 'ros', 'stock', 'bonus_share', 'normal_ros', 'normal_share',
         'paid_ros', 'original_symbol', 'asset_id', 'tse_symbol', 'ros_payday', 'registration_date', 'ros_due_date'],
        dropna=False, as_index=False).sum()
    portfolio = portfolio[['symbol', 'amount', 'total_cost', 'final_price', 'date', 'fund', 'ros', 'stock',
                           'bonus_share', 'normal_ros', 'normal_share', 'paid_ros', 'original_symbol', 'asset_id',
                           'tse_symbol', 'ros_payday', 'registration_date', 'ros_due_date']]

    ##################################################################################

    # updating final_price - if closed (stock -> last final_price) (ros -> stock final_price - 1000)

    syms_ids = portfolio[["tse_symbol"]].merge(pd.concat([symbols, symbols_ros], axis=0, ignore_index=True),
                                               on="tse_symbol", how="left")
    his_sym = pd.read_sql(f"SELECT [date], [symbol_id], [final_price] FROM [nooredenadb].[tsetmc].[symbols_history] "
                          f"WHERE symbol_id IN {str(tuple(syms_ids['symbol_id'].tolist())).replace(',)', ')')} AND "
                          f"date = {today_g_} AND trade_amount > 0", db_conn)
    his_ros = pd.read_sql(f"SELECT [date], [symbol_id], [final_price] FROM [nooredenadb].[tsetmc].[symbols_ros_history] "
                          f"WHERE symbol_id IN {str(tuple(syms_ids['symbol_id'].tolist())).replace(',)', ')')} AND "
                          f"date = {today_g_} AND trade_amount > 0", db_conn)
    history = pd.concat([his_sym, his_ros], axis=0, ignore_index=True)

    history_ = history.merge(syms_ids, on="symbol_id", how="left").drop_duplicates(
        subset="symbol_id", inplace=False, ignore_index=True)[["final_price", "tse_symbol"]]
    history_.rename({"final_price": "price"}, axis=1, inplace=True)
    portfolio = portfolio.merge(history_, on="tse_symbol", how="left")

    portfolio["final_price"] = (((portfolio["price"].isna() & ~portfolio["ros"]) * 1) * portfolio["final_price"]) + \
                                portfolio["price"].fillna(0, inplace=False)

    for i in range(len(portfolio)):
        if portfolio["final_price"].iloc[i] == 0:
            if portfolio["tse_symbol"].iloc[i][:-1] in portfolio["tse_symbol"].values:
                portfolio["final_price"].iloc[i] = portfolio["final_price"].iloc[
                    portfolio.index[portfolio["tse_symbol"] == portfolio["tse_symbol"].iloc[i][:-1]].values[0]] - 1000
            else:
                symbols_tmp = symbols[symbols["tse_symbol"] == portfolio["tse_symbol"].iloc[i][:-1]]
                his_sym_tmp = pd.read_sql(
                    f"SELECT [date], [symbol_id], [final_price] FROM [nooredenadb].[tsetmc].[symbols_history] WHERE "
                    f"symbol_id IN {str(tuple(symbols_tmp['symbol_id'].tolist())).replace(',)', ')')} AND date = "
                    f"{today_g_} AND trade_amount > 0", db_conn)
                portfolio["final_price"].iloc[i] = his_sym_tmp["final_price"].iloc[0] - 1000

    portfolio.drop(columns="price", axis=1, inplace=True)

    ##################################################################################

    # checking for stocks dps announcements (modifying final_price and adding dps to dps_table)

    dps_temp = dps[dps["announcement_date"] == today].reset_index(drop=True, inplace=False)
    dps_temp = dps_temp.merge(
        portfolio[portfolio["normal_share"]][["amount", "symbol", "asset_id"]], on="asset_id", how="left")
    dps_temp.dropna(subset="amount", inplace=True)
    dps_temp["due_date"] = [(jdatetime.datetime.strptime(dps_temp["announcement_date"].iloc[i], "%Y/%m/%d") +
                             (one_day * 120)).strftime("%Y/%m/%d") for i in range(len(dps_temp))]
    dps_temp["paid"] = [0] * len(dps_temp)
    dps_table = pd.concat([dps_table, dps_temp], axis=0, ignore_index=True)

    portfolio = portfolio.merge(dps_temp[["asset_id", 'pure_dps']], on="asset_id", how="left")
    portfolio["pure_dps"].fillna(value=0, inplace=True)
    portfolio["final_price"] = portfolio["final_price"] - portfolio["pure_dps"]
    portfolio.drop(columns="pure_dps", inplace=True, axis=0)

    ##################################################################################

    # checking for funds dps announcements (modifying final_price and adding dps to dps_table)

    dps_funds_temp = dps_funds[dps_funds["date"] == today].reset_index(drop=True, inplace=False)
    dps_funds_temp = dps_funds_temp.merge(
        portfolio[portfolio["fund"]][["amount", "symbol", "asset_id"]], on="asset_id", how="left")
    dps_funds_temp.dropna(subset="amount", inplace=True)
    dps_funds_temp["due_date"] = [(jdatetime.datetime.strptime(dps_funds_temp["date"].iloc[i], "%Y/%m/%d") +
                             (one_day * 14)).strftime("%Y/%m/%d") for i in range(len(dps_funds_temp))]
    dps_funds_temp["paid"] = [0] * len(dps_funds_temp)
    dps_funds_table = pd.concat([dps_funds_table, dps_funds_temp], axis=0, ignore_index=True)
    portfolio = portfolio.merge(dps_funds_temp[["asset_id", 'dividend']], on="asset_id", how="left")
    portfolio["dividend"].fillna(value=0, inplace=True)
    portfolio["final_price"] = portfolio["final_price"] - portfolio["dividend"]
    portfolio.drop(columns="dividend", inplace=True, axis=0)

    ##################################################################################

    # checking for capital changes and (adding ros or bonus and calculating total_cost and final price)

    capital_changes_temp = capital_changes[
        (capital_changes["date"] == today) & (capital_changes["previous_capital"] != capital_changes["new_capital"])
        ].reset_index(drop=True, inplace=False)
    if len(capital_changes_temp) > 0:
        for i in range(len(capital_changes_temp)):
            if capital_changes_temp["asset_id"].iloc[i] in portfolio["asset_id"].values:
                idx_ = portfolio.index[portfolio["asset_id"] == capital_changes_temp["asset_id"].iloc[i]].values[0]

                cap_chng_new_data = capital_increase(
                    CapChangeParams(
                        amount=portfolio["amount"].iloc[idx_],
                        total_cost=portfolio["total_cost"].iloc[idx_],
                        price=portfolio["final_price"].iloc[idx_],
                        old_share=capital_changes_temp["previous_capital"].iloc[i],
                        contribution=capital_changes_temp["contribution"].iloc[i],
                        premium=capital_changes_temp["premium"].iloc[i],
                        reserve=capital_changes_temp["reserve"].iloc[i]
                    )
                )
                portfolio["final_price"].iloc[idx_] = cap_chng_new_data["adj_price"]
                portfolio["total_cost"].iloc[idx_] = cap_chng_new_data["cost_share"]

                if cap_chng_new_data["bonus"] != 0:
                    tmp_ = portfolio.iloc[idx_: idx_ + 1, :].reset_index(drop=True, inplace=False)
                    tmp_["amount"].iloc[0] = cap_chng_new_data["bonus"]
                    tmp_["total_cost"].iloc[0] = cap_chng_new_data["cost_bonus"]
                    tmp_["bonus_share"].iloc[0] = True
                    tmp_["normal_share"].iloc[0] = False
                    tmp_["registration_date"].iloc[0] = capital_changes_temp["registration_date"].iloc[i]
                    portfolio = pd.concat([portfolio, tmp_], axis=0, ignore_index=True)

                if cap_chng_new_data["ros"] != 0:
                    tmp_ = portfolio.iloc[idx_: idx_ + 1, :].reset_index(drop=True, inplace=False)
                    tmp_["symbol"].iloc[0] = tmp_["symbol"].iloc[0] + "ح"
                    tmp_["amount"].iloc[0] = cap_chng_new_data["ros"]
                    tmp_["total_cost"].iloc[0] = cap_chng_new_data["cost_ros"]
                    tmp_["final_price"].iloc[0] = cap_chng_new_data["ros_price"]
                    tmp_["ros"].iloc[0] = True
                    tmp_["bonus_share"].iloc[0] = False
                    tmp_["normal_ros"].iloc[0] = True
                    tmp_["normal_share"].iloc[0] = False
                    tmp_["paid_ros"].iloc[0] = False
                    tmp_["tse_symbol"].iloc[0] = tmp_["tse_symbol"].iloc[0] + "ح"
                    tmp_["registration_date"].iloc[0] = capital_changes_temp["registration_date"].iloc[i]
                    tmp_["ros_payday"].iloc[0] = capital_changes_temp["underwriting_end_date"].iloc[i]
                    tmp_["ros_due_date"].iloc[0] = get_last_date(
                        date_=capital_changes_temp["underwriting_end_date"].iloc[i])
                    portfolio = pd.concat([portfolio, tmp_], axis=0, ignore_index=True)

    ##################################################################################

    # # checking for maturity date of put options
    #
    # for i in range(len(put_options)):
    #     if (put_options["maturity_date"].iloc[i] == today) and (put_options["done"].iloc[i] == False):
    #         if put_options["asset_id"].iloc[i] in portfolio[portfolio["normal_share"]]["asset_id"].values:
    #             idx_ = portfolio.index[(portfolio["asset_id"] == put_options["asset_id"].iloc[i]) &
    #                                    (portfolio["normal_share"])].values[0]
    #             amount = put_options["amount"].iloc[i]
    #             total_cost = np.ceil((portfolio["total_cost"].iloc[idx_] / portfolio["amount"].iloc[idx_]) *
    #                                  put_options["amount"].iloc[i])
    #             strike_price = put_options["strike_price"].iloc[i]
    #             value = strike_price * amount
    #             desc = f"اعمال اختیار فروش تبعی سهم {portfolio['symbol'].iloc[idx_]} ({put_options['symbol'].iloc[i]})"
    #             tmp_ = pd.DataFrame([{"symbol": portfolio["symbol"].iloc[idx_],
    #                                   "date": today,
    #                                   "original_symbol": portfolio["original_symbol"].iloc[idx_],
    #                                   "asset_id": put_options["asset_id"].iloc[i],
    #                                   "tse_symbol": portfolio["tse_symbol"].iloc[idx_], "sell_amount": amount,
    #                                   "strike_price": strike_price, "sell_cost": total_cost, "desc": desc}])
    #             trade_table = pd.concat([trade_table, tmp_],axis=0, ignore_index=True)
    #
    #             portfolio["amount"].iloc[idx_] -= amount
    #             portfolio["total_cost"].iloc[idx_] -= total_cost
    #
    #             portfolio = portfolio[portfolio["amount"] > 0].reset_index(drop=True, inplace=False)

    ##################################################################################

    cashflow_tmp = cashflow[(cashflow["due_date"] == today) & (cashflow["debtor"] > 0)].reset_index(
        drop=True, inplace=False)
    ros_due_tmp = portfolio[portfolio["ros_due_date"] == today].reset_index(drop=True, inplace=False)

    desc = ""
    sell_value = 0
    if (len(cashflow_tmp) != 0) & (len(ros_due_tmp) != 0):
        if (cashflow_tmp["debtor"].iloc[0] + (ros_due_tmp["amount"].sum() * 1000)) > cash_table["remain"].iloc[-1]:
            sell_value = (cashflow_tmp["debtor"].iloc[0] + (ros_due_tmp["amount"].sum() * 1000)
                          ) - cash_table["remain"].iloc[-1]
            desc = f"تامین وجه نقد بابت پرداخت به {cashflow_tmp['source'].iloc[0]} و پرداخت 1000 ریال حق تقدم"
        else:
            pass
    elif (len(cashflow_tmp) != 0) & (len(ros_due_tmp) == 0):
        if (cashflow_tmp["debtor"].iloc[0]) >= cash_table["remain"].iloc[-1]:
            sell_value = (cashflow_tmp["debtor"].iloc[0]) - cash_table["remain"].iloc[-1]
            desc = f"فروش برای تامین وجه نقد بابت پرداخت به {cashflow_tmp['source'].iloc[0]}"
        else:
            pass
    elif (len(cashflow_tmp) == 0) & (len(ros_due_tmp) != 0):
        if ((ros_due_tmp["amount"].sum() * 1000)) > cash_table["remain"].iloc[-1]:
            sell_value = ((ros_due_tmp["amount"].sum() * 1000)) - cash_table["remain"].iloc[-1]
            desc = f"فروش برای تامین وجه نقد بابت پرداخت 1000 ریال حق تقدم"
    else:
        pass


    if (sell_value == 0) and (cash_table["remain"].iloc[-1] > 1e7) and\
            (today_weekday < 5) and (today not in closed_days["date"].values):
        pass
        # if fixed_income_name not in portfolio["symbol"].values:
        #     fixed_income_price = pd.read_sql("SELECT [final_price] as price FROM [nooredenadb].[tsetmc].[symbols_history] "
        #                                      f"WHERE symbol_id='{fixed_income_id}' and date={today_g_}",
        #                                      db_conn)["price"].iloc[0]
        #     fixed_income_df = pd.DataFrame([{'symbol': fixed_income_name, 'amount': 0, 'total_cost': 0, 'date': today,
        #                                      'final_price': fixed_income_price, 'fund': True, 'ros': False,
        #                                      'stock': False, 'bonus_share': False, 'normal_ros': False,
        #                                      'normal_share': True, 'paid_ros': False,
        #                                      'original_symbol': fixed_income_name,
        #                                      'asset_id': "7799", 'tse_symbol': fixed_income_name}])
        #     portfolio = pd.concat([portfolio, fixed_income_df], axis=0, ignore_index=True)
        # else:
        #     pass

        # portfolio["buy_amount"] = (np.floor(cash_table["remain"].iloc[-1] / (portfolio["final_price"]))) * (
        #         portfolio["symbol"] == fixed_income_name)
        # portfolio["buy_cost"] = portfolio["buy_amount"] * portfolio["final_price"]
        # portfolio["amount"] += portfolio["buy_amount"]
        # portfolio["total_cost"] += portfolio["buy_cost"]
        # buy_value = portfolio["buy_cost"].sum()
        #
        # buy_table = portfolio[portfolio["buy_amount"] > 0]
        # buy_table["desc"] = [f"خرید صندوق درآمد ثابت ({fixed_income_name})"] * len(buy_table)
        # buy_table["date"] = today
        # trade_table = pd.concat([trade_table, buy_table], axis=0, ignore_index=True)
        #
        # portfolio.drop(columns=["buy_amount", "buy_cost"], inplace=True)
        #
        # cash_tmp = pd.DataFrame([{"date": today, "desc": f"خرید صندوق درآمد پابت ({fixed_income_name})", "credit": 0,
        #                           "debit": buy_value, "remain": (cash_table["remain"].iloc[-1] - buy_value)}])
        # cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    if sell_value > 0:

        # clearing credit (sell_value = sell_value + credit)

        sell_value = sell_value + credit
        credit = 0
        credit_table["remaining"].iloc[-1] = 0

        # portfolio["sell_amount"] = (np.ceil(sell_value / portfolio["final_price"]) *
        #                             (portfolio["symbol"] == fixed_income_name))
        # portfolio["sell_amount"] = portfolio[["amount", "sell_amount"]].min(axis=1)
        # portfolio["sell_cost"] = np.ceil(portfolio["sell_amount"] * (portfolio["total_cost"] / portfolio["amount"]))
        #
        # sell_table = portfolio[portfolio["sell_amount"] > 0]
        # sell_table["desc"] = [f"فروش صندوق درآمد ثابت ({fixed_income_name})"] * len(sell_table)
        # sell_table["date"] = today
        # trade_table = pd.concat([trade_table, sell_table], axis=0, ignore_index=True)
        #
        # fiv = (portfolio["sell_amount"] * portfolio["final_price"]).sum()
        #
        # portfolio["amount"] -= portfolio["sell_amount"]
        # portfolio["total_cost"] -= portfolio["sell_cost"]
        # portfolio.drop(columns=["sell_amount", "sell_cost"], inplace=True)
        # portfolio = portfolio[portfolio["amount"] > 0].reset_index(drop=True, inplace=False)
        #
        # if fiv > 0:
        #     cash_tmp = pd.DataFrame([{"date": today, "desc": desc, "credit": fiv, "debit": 0,
        #                               "remain": cash_table["remain"].iloc[-1] + fiv}])
        #     cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)
        # else:
        #     pass
        #
        # # Sell fixed income first and if not enough sell portfolio equally weighted
        #
        # sell_value -= fiv

        if sell_value > 0:

            portfolio["sell_amount"] = np.ceil(
                (np.ceil(((portfolio["amount"] * portfolio["final_price"] * portfolio["normal_share"]) /
                          (portfolio["amount"] * portfolio["final_price"] * portfolio["normal_share"]).sum()) * sell_value)
                 ) / portfolio["final_price"])
            portfolio["sell_cost"] = np.ceil((portfolio["sell_amount"] / portfolio["amount"]) * portfolio["total_cost"])

            sell_table = portfolio[portfolio["sell_amount"] > 0]
            sell_table["desc"] = [desc] * len(sell_table)
            sell_table["date"] = today
            trade_table = pd.concat([trade_table, sell_table], axis=0, ignore_index=True)

            portfolio["amount"] -= portfolio["sell_amount"]
            portfolio["total_cost"] -= portfolio["sell_cost"]
            portfolio.drop(columns=["sell_amount", "sell_cost"], inplace=True)

            cash_tmp = pd.DataFrame([{"date": today, "desc": desc,
                                      "credit": (sell_table["sell_amount"] * sell_table["final_price"]).sum(), "debit": 0,
                                      "remain": (cash_table["remain"].iloc[-1] + (
                                              sell_table["sell_amount"] * sell_table["final_price"]).sum())}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

        else:
            pass

    ##################################################################################

    portfolio["date"] = [today] * len(portfolio)
    portfolio_table = pd.concat([portfolio_table, portfolio], axis=0, ignore_index=True)

##################################################################################
##################################################################################

files_address = "D:/database/portfolio simulation/1404-10-30/12-month"

dps_table.to_excel(f"{files_address}/dps_table.xlsx", index=False)
dps_funds_table.to_excel(f"{files_address}/dps_funds_table.xlsx", index=False)
cash_table.to_excel(f"{files_address}/cash_table.xlsx", index=False)
trade_table.to_excel(f"{files_address}/trade_table.xlsx", index=False)
portfolio_table.to_excel(f"{files_address}/portfolio_table.xlsx", index=False)
credit_table.to_excel(f"{files_address}/credit_table.xlsx", index=False)

trade_table["final_price"].fillna(value=0, inplace=True)

if "strike_price" not in trade_table.columns.values.tolist():
    trade_table["strike_price"] = None
trade_table["strike_price"].fillna(value=0, inplace=True)
trade_table["value"] = (trade_table["final_price"] + trade_table["strike_price"]) * trade_table["sell_amount"]
trade_table["profit"] = trade_table["value"] - trade_table["sell_cost"]

trade_table["date_month"] = trade_table["date"].str[:-3]
trade_table_df = trade_table[["date_month", "profit"]].groupby(by="date_month", as_index=False).sum()

dps_table_df = pd.concat(
    [dps_funds_table[["date", "symbol", "amount", "dividend"]].rename({"dividend": "pure_dps"}, axis=1, inplace=False),
     dps_table[["announcement_date", "symbol", "amount", "pure_dps"]].rename(
         {"announcement_date": "date"}, axis=1, inplace=False)], axis=0, ignore_index=True)
dps_table_df["date_month"] = dps_table_df["date"].str[:-3]
dps_table_df["dividend"] = dps_table_df["pure_dps"] * dps_table_df["amount"]
dps_table_df = dps_table_df[["date_month", "dividend"]].groupby(by="date_month", as_index=False).sum()

portfolio_table["value"] = portfolio_table["amount"] * portfolio_table["final_price"]
portfolio_table_df = portfolio_table[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()
portfolio_table_df["year-month"] = portfolio_table_df["date"].str[:7]
portfolio_table_df_ = portfolio_table_df[["year-month", "date"]].groupby(by="year-month", as_index=False).max()
portfolio_table_df = portfolio_table_df_[["date"]].merge(portfolio_table_df[["date", "value", "total_cost"]],
                                                         on="date", how="left")

cash_table["row"] = range(len(cash_table))
cash_table_df = cash_table[["row", "date", "remain"]]
cash_table_df["year-month"] = cash_table_df["date"].str[:7]
cash_table_df_ = cash_table_df[["year-month", "row", "date"]].groupby(by="year-month", as_index=False).max()
cash_table_df = cash_table_df_[["date", "row"]].merge(cash_table_df[["date", "row", "remain"]],
                                                         on=["date", "row"], how="left")[["date", "remain"]]

cash_table_df["date_"] = cash_table_df["date"].str[:7]
portfolio_table_df["date_"] = portfolio_table_df["date"].str[:7]
portfolio_table_df = portfolio_table_df.merge(
    cash_table_df.drop(columns="date", inplace=False), on="date_", how="left").drop(columns="date_", inplace=False)
cash_table_df.drop(columns="date_", inplace=True)

dps_table_df["dividend"] /= 1e9
trade_table_df["profit"] /= 1e9
portfolio_table_df["value"] /= 1e9
portfolio_table_df["total_cost"] /= 1e9
portfolio_table_df["remain"] /= 1e9
cash_table_df["remain"] /= 1e9

dps_table_df.to_excel(f"{files_address}/cumulative/dps_table_df.xlsx", index=False)
trade_table_df.to_excel(f"{files_address}/cumulative/trade_table_df.xlsx", index=False)
portfolio_table_df.to_excel(f"{files_address}/cumulative/portfolio_table_df.xlsx", index=False)
cash_table_df.to_excel(f"{files_address}/cumulative/cash_table_df.xlsx", index=False)
