import warnings
import jdatetime
import numpy as np
import pandas as pd

from utils.database import make_connection
from trade_intel.utils.funcs import relu, get_last_date, get_next_date, capital_increase, CapChangeParams


start_day = "1403/10/30"
end_day = "1404/05/01"
warnings.filterwarnings("ignore")
one_day = jdatetime.timedelta(days=1)
db_conn = make_connection()
fixed_income_id = "3846143218462419"

query1 = "SELECT trades_rayan.row_, trades_rayan.transactionDate as date, trades_rayan.remaining, " \
         "trades_rayan.broker_id FROM [nooredenadb].[brokers].[trades_rayan] INNER JOIN " \
         "(SELECT max(row_) as row__, transactionDate as date, broker_id FROM [nooredenadb].[brokers].[trades_rayan] " \
         "where transactionDate >= '1402/01/01' group by transactionDate, broker_id) as temp ON row__=row_ ORDER BY " \
         "date, broker_id"
credits1 = pd.read_sql(query1, db_conn)
credits1.drop(columns=["row_"], inplace=True)

query2 = "SELECT temp.row_, temp.broker_id, temp.date_, trades_tadbir_ledger.Remain as remaining FROM " \
         "(SELECT max(row_) as row_, broker_id, substring(TransactionDate, 1, 10) as date_ FROM " \
         "[nooredenadb].[brokers].[trades_tadbir_ledger] where TransactionDate >= '2023-03-21' and Description not " \
         "in ('سند اختتامیه', 'سند افتتاحیه مورخ {0}') group by substring(TransactionDate, 1, 10), broker_id) as " \
         "temp INNER JOIN [nooredenadb].[brokers].[trades_tadbir_ledger] ON temp.row_=trades_tadbir_ledger.row_"
credits2 = pd.read_sql(query2, db_conn)

query3 = "SELECT [Jalali_1] as date, [Miladi] as date_ FROM [nooredenadb].[extra].[dim_date] " \
         "WHERE Jalali_1 >= '1402/01/01' AND Jalali_1 <= '1404/08/30' AND MWeekDay NOT IN ('Thursday', 'Friday')"
dim_date = pd.read_sql(query3, db_conn)
dim_date["date_"] = dim_date["date_"].astype("str")

credits2 = credits2.merge(dim_date, on="date_", how="left").drop(columns=["date_", "row_"], inplace=False)
credits = pd.concat([credits1, credits2], axis=0, ignore_index=True)
credits_df = pd.DataFrame()
for broker in credits["broker_id"].unique().tolist():
    dim_date["broker_id"] = broker
    df = dim_date.merge(credits[["date", "broker_id", "remaining"]], on=["date", "broker_id"], how="left")
    df["remaining"] = df["remaining"].fillna(method="ffill", inplace=False).fillna(value=0, inplace=False)
    credits_df = pd.concat([credits_df, df], axis=0, ignore_index=True)

credits_df["remaining"] = ((credits_df["remaining"] < 0) * 1) * credits_df["remaining"]
credits_df = credits_df[["date", "remaining"]].groupby(by="date", as_index=False).sum()
credits_df = credits_df[credits_df["date"] >= "1403/10/30"].reset_index(drop=True, inplace=False)

credits_df["remaining"] = [credits_df["remaining"].iloc[0]] + [0] * (len(credits_df) - 1)


capital_increase(CapChangeParams(amount=1_000_000,
                 total_cost=13_770_000_000,
                 price=13_770,
                 old_share=75_000_000_000,
                 contribution=0,
                 premium=3_528_937_286,
                 reserve=31_471_062_714))

capital_increase(CapChangeParams(amount=1_000_000,
                 total_cost=9_080_000_000,
                 price=9_080,
                 old_share=110_000_000_000,
                 contribution=0,
                 premium=5_776_173_285,
                 reserve=34_223_826_715))

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

cashflow = pd.read_sql("SELECT * FROM [nooredenadb].[company].[cashflow]", db_conn)
cashflow["due_date"] = [get_last_date(date_=cashflow["date"].iloc[i]) if ~cashflow["date"].isna().iloc[i] else np.nan
                        for i in range(len(cashflow))]
cashflow["paid"] = False

put_options = pd.read_sql("SELECT * FROM [nooredenadb].[sigma].[put_option]", db_conn)
put_options["payday"] = [get_next_date(date_=put_options["maturity_date"].iloc[i]) for i in range(len(put_options))]
put_options["done"] = False

rahavard_symbols = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[symbols]", db_conn)
portfolio = pd.read_sql("SELECT [symbol], [type], [status], [amount], [total_cost], [final_price], [date] "
                        "FROM [nooredenadb].[sigma].[portfolio] WHERE date = (SELECT MAX(date) FROM "
                        "[nooredenadb].[sigma].[portfolio] WHERE date < '1402/11/01')", db_conn)
portfolio["type"].replace({"اختیار معامله | موقعیت فروش": "put_option", "اختیار معامله | موقعیت خرید": "call_option",
                           "صندوق": "fund", "سلف موازی": "forward_contract", "گواهی سپرده کالایی": "commodity_cd",
                           "حق تقدم": "ros", "اوراق": "bond", "سهام": "stock"}, inplace=True, regex=False)
portfolio["status"].replace({r"حق\u200cتقدم پذیره نویسی شده": "paid_ros", "سهام جایزه": "bonus_share",
                             "حق تقدم عادی": "normal_ros", "حق تقدم استفاده نشده": "unused_ros",
                             "سهام عادی": "normal_share", "عادی": "normal_share"}, inplace=True, regex=False)
portfolio["status"].iloc[portfolio.index[portfolio["symbol"] == "آکنتور"].values[0]] = "bonus_share"
portfolio = pd.concat([portfolio, pd.get_dummies(portfolio["type"]), pd.get_dummies(portfolio["status"])],
                       axis=1).drop(columns=["type", "status"], inplace=False)
portfolio = portfolio[~portfolio["call_option"] & ~portfolio["put_option"]].drop(columns=["call_option", "put_option"])
portfolio['original_symbol'] = portfolio.apply(lambda row: row['symbol'][:len(row["symbol"]) + (row["normal_ros"] * -1)], axis=1)
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

credit_table = pd.DataFrame([{"date": start_day, "remaining": credits_df["remaining"].iloc[0]}])
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
    tomorrow = today + one_day
    today_g = jdatetime.datetime.togregorian(today)

    today = today.strftime("%Y/%m/%d")
    tomorrow = tomorrow.strftime("%Y/%m/%d")
    today_g = today_g.strftime("%Y/%m/%d")
    today_g_ = int(today_g.replace("/", ""))

    if today >= end_day:
        break
    print(f"Loading portfolio {today}")

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
        if (cashflow["date"].iloc[i] == today) and (cashflow["paid"].iloc[i] == False) and (cashflow["debtor"].iloc[i] > 0):
            cashflow["paid"].iloc[i] = True
            cash_tmp = pd.DataFrame([{"date": today, "desc": cashflow["description"].iloc[i], "credit": 0,
                                      "debit": cashflow["debtor"].iloc[i], "remain": (
                        cash_table["remain"].iloc[-1] - cashflow["debtor"].iloc[i])}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)
        if (cashflow["date"].iloc[i] == today) and (cashflow["paid"].iloc[i] == False) and (cashflow["creditor"].iloc[i] > 0):
            cashflow["paid"].iloc[i] = True
            cash_tmp = pd.DataFrame([{"date": today, "desc": cashflow["description"].iloc[i],
                                      "credit": cashflow["creditor"].iloc[i], "debit": 0,
                                      "remain": (cash_table["remain"].iloc[-1] + cashflow["creditor"].iloc[i])}])
            cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)

    ##################################################################################

    # # checking for srtoke put options payments and adding to cash_table
    # put_opt_tmp = put_options[(put_options["payday"] == today) & (put_options["done"])].reset_index(drop=True, inplace=False)
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
    #                                   # "final_price": portfolio["final_price"].iloc[idx_],
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

    cashflow_tmp = cashflow[(cashflow["due_date"] == today) & (cashflow["debtor"] > 0)].reset_index(drop=True, inplace=False)
    ros_due_tmp = portfolio[portfolio["ros_due_date"] == today].reset_index(drop=True, inplace=False)

    credit_today = credits_df[credits_df["date"] <= today]["remaining"].iloc[-1]
    credit_change = credit_today - credit_table["remaining"].iloc[-1]
    credit_change = relu(credit_change)
    desc_credit = " و تعدیل اعتبار"
    desc_credit = desc_credit * (credit_change > 0)

    desc = ""
    sell_value = 0
    if (len(cashflow_tmp) != 0) & (len(ros_due_tmp) != 0):
        if (cashflow_tmp["debtor"].iloc[0] + (ros_due_tmp["amount"].sum() * 1000) + credit_change) > cash_table["remain"].iloc[-1]:
            sell_value = (cashflow_tmp["debtor"].iloc[0] + (ros_due_tmp["amount"].sum() * 1000) + credit_change) - \
                         cash_table["remain"].iloc[-1]
            desc = f"تامین وجه نقد بابت پرداخت به {cashflow['source'].iloc[0]} و پرداخت 1000 ریال حق تقدم" + desc_credit
        else:
            pass
    elif (len(cashflow_tmp) != 0) & (len(ros_due_tmp) == 0):
        if (cashflow_tmp["debtor"].iloc[0] + credit_change) >= cash_table["remain"].iloc[-1]:
            sell_value = (cashflow_tmp["debtor"].iloc[0] + credit_change) - cash_table["remain"].iloc[-1]
            desc = f"فروش برای تامین وجه نقد بابت پرداخت به {cashflow['source'].iloc[0]}" + desc_credit
        else:
            pass
    elif (len(cashflow_tmp) == 0) & (len(ros_due_tmp) != 0):
        if ((ros_due_tmp["amount"].sum() * 1000) + credit_change) > cash_table["remain"].iloc[-1]:
            sell_value = ((ros_due_tmp["amount"].sum() * 1000) + credit_change) - cash_table["remain"].iloc[-1]
            desc = f"فروش برای تامین وجه نقد بابت پرداخت 1000 ریال حق تقدم" + desc_credit
    else:
        if credit_change > cash_table["remain"].iloc[-1]:
            sell_value = credit_change - cash_table["remain"].iloc[-1]
            desc = "فروش برای تعدیل اعتبار"

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

    if credit_change > 0:
        cash_tmp = pd.DataFrame([{"date": today, "desc": "برداشت از حساب برای تعدیل اعتبار",
                                  "credit": 0,
                                  "debit": credit_change,
                                  "remain": (cash_table["remain"].iloc[-1] - credit_change)}])
        cash_table = pd.concat([cash_table, cash_tmp], axis=0, ignore_index=0)
    else:
        pass

    credit_table = pd.concat([credit_table, pd.DataFrame([{"date": today, "remaining": credit_today}])], axis=0,
                             ignore_index=True)

    ##################################################################################

    portfolio["date"] = [today] * len(portfolio)
    portfolio_table = pd.concat([portfolio_table, portfolio], axis=0, ignore_index=True)

##################################################################################
##################################################################################

dps_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/dps_table.xlsx", index=False)
dps_funds_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/dps_funds_table.xlsx", index=False)
cash_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/cash_table.xlsx", index=False)
trade_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/trade_table.xlsx", index=False)
portfolio_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/portfolio_table.xlsx", index=False)
credit_table.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/credit_table.xlsx", index=False)

trade_table["final_price"].fillna(value=0, inplace=True)
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

credit_table["remaining-diff"] = credit_table["remaining"].diff().fillna(value=0, inplace=False)
credit_table["remaining-diff-negative"] = ((credit_table["remaining-diff"] < 0) * 1) * credit_table["remaining-diff"]
credit_table["remaining-diff-negative-cumulative"] = credit_table["remaining-diff-negative"].cumsum() * (-1)

portfolio_table_df = portfolio_table_df.merge(credit_table[["date", "remaining-diff-negative-cumulative"]], on="date", how="left")

cash_table_df["date_"] = cash_table_df["date"].str[:7]
portfolio_table_df["date_"] = portfolio_table_df["date"].str[:7]
portfolio_table_df = portfolio_table_df.merge(
    cash_table_df.drop(columns="date", inplace=False), on="date_", how="left").drop(columns="date_", inplace=False)
cash_table_df.drop(columns="date_", inplace=True)

dps_table_df["dividend"] /= 1e9
dps_table_df.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/dps_table_df.xlsx", index=False)

trade_table_df["profit"] /= 1e9
trade_table_df.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/trade_table_df.xlsx", index=False)

portfolio_table_df["value"] /= 1e9
portfolio_table_df["total_cost"] /= 1e9
portfolio_table_df["remaining-diff-negative-cumulative"] /= 1e9
portfolio_table_df["remain"] /= 1e9
portfolio_table_df.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/portfolio_table_df.xlsx", index=False)

cash_table_df["remain"] /= 1e9
cash_table_df.to_excel("C:/Users/damavandi.NOOREDENA/Desktop/cash_table_df.xlsx", index=False)
