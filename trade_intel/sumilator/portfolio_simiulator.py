import warnings
import jdatetime
import numpy as np
import pandas as pd

from utils.database import make_connection
from trade_intel.sumilator.utils.credit.credit_history import get_credits_history
from trade_intel.sumilator.utils.funcs import (
    capital_increase,
    CapChangeParams
)
from trade_intel.sumilator.utils.data import (
    get_closed_dates,
    get_last_date,
    get_fundamental_data,
    get_cashflow,
    get_symbols,
    get_symbols_ros,
    get_start_portfolio,
)



warnings.filterwarnings("ignore")
one_day = jdatetime.timedelta(days=1)
credit_APR = 0.28
start_day = "1404/10/30"
end_day = "1405/08/01"

db_conn = make_connection()
if not db_conn:
    raise ConnectionError("Database connection failed")

################################################################################################

symbols = get_symbols(db_connection=db_conn)
symbols_ros = get_symbols_ros(db_connection=db_conn)
credit = get_credits_history(start_date=start_day, end_date=end_day, db_connection=db_conn)
credit = credit[credit["date"] == start_day]["credit"].values.tolist()[0]
closed_days = get_closed_dates(db_connection=db_conn)
cashflow = get_cashflow(db_connection=db_conn, start_date=start_day, include_avalhami_redemption=False)

fundamental_data = get_fundamental_data(db_connection=db_conn)
dps_funds = fundamental_data["dps_funds"]
dps = fundamental_data["dps"]
capital_changes = fundamental_data["capital_changes"]

start_date_data = get_start_portfolio(start_date=start_day, capital_changes_df=capital_changes, db_connection=db_conn)
portfolio = start_date_data["portfolio"]
portfolio_value_start = start_date_data["value"]

################################################################################################

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
        ['symbol', 'final_price', 'date', 'ros', 'stock', 'bonus_share', 'normal_ros', 'normal_share',
         'paid_ros', 'original_symbol', 'asset_id', 'tse_symbol', 'ros_payday', 'registration_date', 'ros_due_date'],
        dropna=False, as_index=False).sum()
    portfolio = portfolio[['symbol', 'amount', 'total_cost', 'final_price', 'date', 'ros', 'stock',
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
        portfolio[["amount", "symbol", "asset_id"]], on="asset_id", how="left")
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
                        date_=capital_changes_temp["underwriting_end_date"].iloc[i],
                        db_connection=db_conn
                    )
                    portfolio = pd.concat([portfolio, tmp_], axis=0, ignore_index=True)

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

    if sell_value > 0:

        # clearing credit (sell_value = sell_value + credit)

        sell_value = sell_value + credit
        credit = 0
        credit_table["remaining"].iloc[-1] = 0

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


files_address = "D:/database/portfolio simulation/1405-10-30/09-month"

dps_table.to_excel(f"{files_address}/dps_table.xlsx", index=False)
dps_funds_table.to_excel(f"{files_address}/dps_funds_table.xlsx", index=False)
cash_table.to_excel(f"{files_address}/cash_table.xlsx", index=False)
trade_table.to_excel(f"{files_address}/trade_table.xlsx", index=False)
portfolio_table.to_excel(f"{files_address}/portfolio_table.xlsx", index=False)
credit_table.to_excel(f"{files_address}/credit_table.xlsx", index=False)


if not trade_table.empty:
    trade_table["final_price"].fillna(value=0, inplace=True)

    if "strike_price" not in trade_table.columns.values.tolist():
        trade_table["strike_price"] = None
    trade_table["strike_price"].fillna(value=0, inplace=True)
    trade_table["value"] = (trade_table["final_price"] + trade_table["strike_price"]) * trade_table["sell_amount"]
    trade_table["profit"] = trade_table["value"] - trade_table["sell_cost"]

    trade_table["date_month"] = trade_table["date"].str[:-3]
    trade_table_df = trade_table[["date_month", "profit"]].groupby(by="date_month", as_index=False).sum()
    trade_table_df["profit"] /= 1e9
else:
    trade_table_df = pd.DataFrame()


if dps_table.empty and dps_funds_table.empty:
    dps_table_df = pd.DataFrame()
else:
    dps_table_df = pd.concat(
        [dps_funds_table[["date", "symbol", "amount", "dividend"]].rename({"dividend": "pure_dps"}, axis=1, inplace=False),
         dps_table[["announcement_date", "symbol", "amount", "pure_dps"]].rename(
             {"announcement_date": "date"}, axis=1, inplace=False)], axis=0, ignore_index=True)
    dps_table_df["date_month"] = dps_table_df["date"].str[:-3]
    dps_table_df["dividend"] = dps_table_df["pure_dps"] * dps_table_df["amount"]
    dps_table_df = dps_table_df[["date_month", "dividend"]].groupby(by="date_month", as_index=False).sum()
    dps_table_df["dividend"] /= 1e9


if not portfolio_table.empty:
    portfolio_table["value"] = portfolio_table["amount"] * portfolio_table["final_price"]
    portfolio_table_df = portfolio_table[["date", "value", "total_cost"]].groupby(by="date", as_index=False).sum()
    portfolio_table_df["year-month"] = portfolio_table_df["date"].str[:7]
    portfolio_table_df_ = portfolio_table_df[["year-month", "date"]].groupby(by="year-month", as_index=False).max()
    portfolio_table_df = portfolio_table_df_[["date"]].merge(portfolio_table_df[["date", "value", "total_cost"]],
                                                             on="date", how="left")
    portfolio_table_df["value"] /= 1e9
    portfolio_table_df["total_cost"] /= 1e9
else:
    portfolio_table_df = pd.DataFrame()



if not cash_table.empty:
    cash_table["row"] = range(len(cash_table))
    cash_table_df = cash_table[["row", "date", "remain"]]
    cash_table_df["year-month"] = cash_table_df["date"].str[:7]
    cash_table_df_ = cash_table_df[["year-month", "row", "date"]].groupby(by="year-month", as_index=False).max()
    cash_table_df = cash_table_df_[["date", "row"]].merge(cash_table_df[["date", "row", "remain"]],
                                                             on=["date", "row"], how="left")[["date", "remain"]]
    cash_table_df["remain"] /= 1e9

    cash_table_df["date_"] = cash_table_df["date"].str[:7]
    portfolio_table_df["date_"] = portfolio_table_df["date"].str[:7]
    portfolio_table_df = portfolio_table_df.merge(
        cash_table_df.drop(columns="date", inplace=False), on="date_", how="left").drop(columns="date_", inplace=False)
    cash_table_df.drop(columns="date_", inplace=True)
else:
    cash_table_df = pd.DataFrame()



dps_table_df.to_excel(f"{files_address}/cumulative/dps_table_df.xlsx", index=False)
trade_table_df.to_excel(f"{files_address}/cumulative/trade_table_df.xlsx", index=False)
portfolio_table_df.to_excel(f"{files_address}/cumulative/portfolio_table_df.xlsx", index=False)
cash_table_df.to_excel(f"{files_address}/cumulative/cash_table_df.xlsx", index=False)
