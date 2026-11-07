import warnings
import pandas as pd
from pyodbc import Connection
from pandas.core.frame import DataFrame

from utils.database import make_connection
from portfolio_return.credit.credit_history import get_credits_history



def handle_timeframe(timeframe: str) -> None:
    if timeframe == "daily":
        pass
    elif timeframe == "weekly":
        pass
    elif timeframe == "monthly":
        pass
    else:
        raise ValueError(f"timeframe {timeframe} is not supported, timeframe should be 'daily' or 'monthly'")


def get_dim_date(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    if timeframe == "monthly":
        q_ = (f"SELECT Jalali_1 date, MAX(Jalali_1) OVER (PARTITION BY LEFT(Jalali_1, 7)) AS {timeframe} FROM "
              f"[nooredenadb].[extra].[dim_date] WHERE Jalali_1 > '{start_date}' AND Jalali_1 <= '{end_date}';")
    else:
        q_ = (f"SELECT Jalali_1 date, MAX(Jalali_1) OVER (PARTITION BY jyear, JWeekNum) AS {timeframe} FROM "
              f"[nooredenadb].[extra].[dim_date] WHERE Jalali_1 > '{start_date}' AND Jalali_1 <= '{end_date}';")

    dim_date = pd.read_sql(q_, db_connection)

    return dim_date


def get_cashflow(db_connection: Connection) -> DataFrame:
    cashflow = pd.read_sql(
        "SELECT date, SUM (debtor) debtor FROM [nooredenadb].[company].[cashflow] "
        "WHERE source = 'ارتباطات سیار' AND description not like '%افزایش سرمایه%' GROUP BY date",
        db_connection
    )
    return cashflow


def get_portfolio(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    portfolio = pd.read_sql(
        "SELECT date,SUM(gross_value_final_price) value,SUM(total_cost) total_cost "
        "FROM [nooredenadb].[sigma].[portfolio] WHERE "
        "type NOT IN ('اختیار معامله | موقعیت خرید', 'اختیار معامله | موقعیت فروش') AND "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date >= '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        portfolio = portfolio.merge(dim_date, on="date", how="left")
        portfolio.fillna({timeframe: portfolio["date"]}, inplace=True)
        portfolio = portfolio[portfolio[timeframe] == portfolio["date"]]
        portfolio.drop(columns=[timeframe], inplace=True)

    return portfolio


def get_profitloss(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    profitloss = pd.read_sql(
        "SELECT date, SUM(net_profit) net_profit FROM [nooredenadb].[sigma].[profitloss] WHERE "
        # f"type != 'گواهی سپرده کالایی' AND "
        f"date > '{start_date}' AND date <= '{end_date}' GROUP BY date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        profitloss = profitloss.merge(dim_date, on="date", how="left")
        profitloss = profitloss.groupby(by=timeframe, as_index=False).sum(numeric_only=True)
        profitloss = profitloss.rename(columns={timeframe: "date"}, inplace=False)

    return profitloss


def get_dividend(start_date: str, end_date: str, timeframe: str, db_connection: Connection) -> pd.DataFrame:

    handle_timeframe(timeframe)

    dividend = pd.read_sql(
        "SELECT meeting_date date, SUM(value) dividend FROM [nooredenadb].[sigma].[dividend] "
        f"WHERE meeting_date > '{start_date}' AND meeting_date <= '{end_date}' GROUP BY meeting_date",
        db_connection
    )

    if timeframe != "daily":
        dim_date = get_dim_date(
            start_date=start_date, end_date=end_date, timeframe=timeframe, db_connection=db_connection
        )
        dividend = dividend.merge(dim_date, on="date", how="left")
        dividend = dividend.groupby(by=timeframe, as_index=False).sum(numeric_only=True)
        dividend = dividend.rename(columns={timeframe: "date"}, inplace=False)

    return dividend



def get_total_index(db_connection: Connection) -> DataFrame:
    total_index_history = pd.read_sql(
        """
        SELECT
            dd.Jalali_1 date,
            ih.close_price total_index
        FROM nooredenadb.tsetmc.indices_history AS ih
        LEFT JOIN nooredenadb.extra.dim_date dd 
        ON
            TRY_CONVERT(INT, REPLACE(dd.Miladi, '-', '')) = ih.date
        WHERE
            ih.index_id = N'32097828799138957'
        ORDER BY
            dd.Jalali_1 

        """,
        db_connection
    )
    return total_index_history


def get_portfolio_return(
        start_date: str,
        end_date: str,
        timeframe: str,
        db_connection: Connection,
        save_to_excel: bool = False,
) -> DataFrame:

    # timeframe = "daily"
    # start_date = "1404/10/30"
    # end_date = "1405/07/30"
    # db_connection = db_conn
    # save_to_excel = False

    cashflow = get_cashflow(db_connection=db_connection)
    portfolio_df = get_portfolio(start_date, end_date, timeframe, db_connection)
    profitloss_df = get_profitloss(start_date, end_date, timeframe, db_connection)
    dividend_df = get_dividend(start_date, end_date, timeframe, db_connection)


    df = portfolio_df.merge(profitloss_df, how="outer", on="date").merge(dividend_df, how="outer", on="date")
    # df = df.merge(cashflow, on="date", how="outer")

    df.fillna(0, inplace=True)
    # df["surplus"] = df["value"] - df["total_cost"] + df["dividend"] + df["net_profit"]
    # df["surplus_yesterday"] = df["value"].shift(1) - df["total_cost"].shift(1)
    df["return"] = ((df["value"] - df["total_cost"] + df["dividend"] + df["net_profit"]) -
                    (df["value"].shift(1) - df["total_cost"].shift(1))) / df["value"].shift(1)
    df["return"] += 1

    df.dropna(subset=["return"], inplace=True)
    # df['cum_return_forward'] = df['return'].iloc[::-1].cumprod().iloc[::-1]
    # df['debtor_times_cum_return'] = (df['debtor'] * df['cum_return_forward']).round(0)
    if save_to_excel:
        df.to_excel("c:/users/h.damavandi/desktop/df.xlsx", index=False)
    return df




warnings.filterwarnings("ignore")
db_conn = make_connection()

df = get_portfolio_return(
    timeframe="daily",
    start_date="1404/10/30",
    end_date="1405/07/30",
    db_connection=db_conn,
    save_to_excel=False
)

total_index = get_total_index(
    db_connection=db_conn
)

total_index["return"] = total_index["total_index"] / total_index["total_index"].shift(1)
df_ = df.rename(mapper={"return": "portfolio_return"}, axis=1).merge(
    total_index.rename(mapper={"return": "total_index_return"}, axis=1),
    on="date",
    how="left"
)[["date", "portfolio_return", "total_index_return"]]

df_["portfolio_return"] = ((df_["portfolio_return"] - 1) * 100).round(2)
df_["total_index_return"] = ((df_["total_index_return"] - 1) * 100).round(2)




# import numpy as np
# import pandas as pd

# df_clean = df_.dropna(subset=['portfolio_return', 'total_index_return']).copy()
# df_clean = df_clean[(df_clean["date"] < "1404/12/09") | (df_clean["date"] > "1405/03/28")]
#
# df_clean.to_csv("c:/users/h.damavandi/desktop/df.csv", index=False)

# df_clean['excess_return'] = (
#     df_clean['portfolio_return'] - df_clean['total_index_return']
# )
#
# market_up = df_clean[df_clean['total_index_return'] > 0]
# print('--- وضعیت روزهای مثبت بازار ---')
# print(f'تعداد روزهای مثبت: {len(market_up)}')
# print(
#     'میانگین بازدهی پورتفوی در روزهای مثبت:',
#     market_up['portfolio_return'].mean(),
# )
# print(
#     'میانگین بازدهی شاخص در روزهای مثبت:',
#     market_up['total_index_return'].mean(),
# )
# print(
#     'درصدی از روزهای مثبت که پرتفوی از شاخص عقب افتاده:',
#     (market_up['excess_return'] < 0).mean() * 100,
# )
#
# # تفکیک روزهای مثبت بالا (مثلاً > 1.5 درصد)
# market_high_up = df_clean[df_clean['total_index_return'] > 1.5]
# print('\n--- وضعیت روزهای مثبت شدید بازار (> 1.5%) ---')
# print(f'تعداد روزها: {len(market_high_up)}')
# print(
#     'درصد دفعاتی که پرتفوی بازدهی کمتری از بازار داشته:',
#     (market_high_up['excess_return'] < 0).mean() * 100,
# )
#
# # ۲. بررسی روزهای منفی شدید بازار (مثلاً < -1.5 درصد)
# market_high_down = df_clean[df_clean['total_index_return'] < -1.5]
# print('\n--- وضعیت روزهای منفی شدید بازار (< -1.5%) ---')
# print(f'تعداد روزها: {len(market_high_down)}')
# print(
#     'درصد دفعاتی که پرتفوی عملکرد بهتری داشته (بازدهی پرتفوی > بازدهی شاخص):',
#     (market_high_down['portfolio_return'] > market_high_down['total_index_return'])
#     .mean()
#     * 100,
# )
#
# # ۳. محاسبه بتا (Beta) و آلفا (Alpha) با رگرسیون ساده خطی
# covariance = np.cov(
#     df_clean['portfolio_return'], df_clean['total_index_return']
# )[0, 1]
# market_variance = np.var(df_clean['total_index_return'])
# beta = covariance / market_variance
# alpha = df_clean['portfolio_return'].mean() - beta * df_clean[
#     'total_index_return'
# ].mean()
#
# print('\n--- پارامترهای ریسک و بازده کلی ---')
# print(f'Beta پورتفوی نسبت به شاخص: {beta:.4f}')
# print(f'Alpha (بازده مازاد ساختاری): {alpha:.4f}')







import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

data = df_.dropna(subset=['portfolio_return', 'total_index_return']).copy()
data = data[(data["date"] < "1404/12/09") | (data["date"] > "1405/03/28")]


data["excess_return"] = (
    data["portfolio_return"] - data["total_index_return"]
)

data["outperformed"] = data["excess_return"] > 0

data["market_up"] = data["total_index_return"] > 0
data["market_down"] = data["total_index_return"] < 0

bins = [-np.inf, -2, -1, 0, 1, 2, np.inf]

labels = [
    "Strong Down",
    "Moderate Down",
    "Mild Down",
    "Mild Up",
    "Moderate Up",
    "Strong Up",
]

data["market_regime"] = pd.cut(
    data["total_index_return"],
    bins=bins,
    labels=labels,
    include_lowest=True,
)

regime_stats = (
    data.groupby("market_regime", observed=True)
    .agg(
        days=("excess_return", "size"),
        avg_portfolio_return=("portfolio_return", "mean"),
        avg_index_return=("total_index_return", "mean"),
        avg_excess_return=("excess_return", "mean"),
        median_excess_return=("excess_return", "median"),
        win_rate=("outperformed", "mean"),
        excess_std=("excess_return", "std"),
    )
)

regime_stats["win_rate"] *= 100

print(regime_stats.round(3))



thresholds = [-0.5, -1, -1.5, -2, -2.5, -3]

results = []

for threshold in thresholds:
    subset = data.loc[
        data["total_index_return"] <= threshold
    ]

    if subset.empty:
        continue

    results.append({
        "threshold": threshold,
        "days": len(subset),
        "win_rate": subset["outperformed"].mean() * 100,
        "avg_excess": subset["excess_return"].mean(),
        "median_excess": subset["excess_return"].median(),
        "avg_portfolio": subset["portfolio_return"].mean(),
        "avg_index": subset["total_index_return"].mean(),
    })

threshold_stats = pd.DataFrame(results)

print(threshold_stats.round(3))


from scipy.stats import binomtest, ttest_1samp

def test_outperformance(subset):
    excess = subset["excess_return"].dropna()

    n = len(excess)
    wins = (excess > 0).sum()

    if n == 0:
        return None

    binom_pvalue = binomtest(
        k=wins,
        n=n,
        p=0.5,
        alternative="greater",
    ).pvalue

    t_pvalue = (
        ttest_1samp(
            excess,
            popmean=0,
            alternative="greater",
        ).pvalue
        if n >= 2 and excess.std() > 0
        else np.nan
    )

    return {
        "n_days": n,
        "win_rate": wins / n * 100,
        "mean_excess": excess.mean(),
        "binomial_pvalue": binom_pvalue,
        "t_test_pvalue": t_pvalue,
    }


conditions = {
    "All Days": data,
    "Market Up": data[data["total_index_return"] > 0],
    "Market Down": data[data["total_index_return"] < 0],
    "Market Below -1%": data[data["total_index_return"] <= -1],
    "Market Below -1.5%": data[data["total_index_return"] <= -1.5],
    "Market Above 1%": data[data["total_index_return"] >= 1],
}

stat_results = pd.DataFrame({
    name: test_outperformance(subset)
    for name, subset in conditions.items()
}).T

print(stat_results.round(4))



plt.figure(figsize=(11, 7))

sns.regplot(
    data=data,
    x="total_index_return",
    y="portfolio_return",
    scatter_kws={"alpha": 0.55},
    line_kws={"color": "red"},
)

lims = [
    min(data["total_index_return"].min(),
        data["portfolio_return"].min()),
    max(data["total_index_return"].max(),
        data["portfolio_return"].max()),
]

plt.plot(
    lims,
    lims,
    linestyle="--",
    color="gray",
    label="Portfolio = Index",
)

plt.xlabel("Index Daily Return (%)")
plt.ylabel("Portfolio Daily Return (%)")
plt.title("Portfolio vs Market Performance")
plt.legend()
plt.grid(alpha=0.2)
plt.show()



def compounded_return_percent(returns):
    return (
        np.prod(1 + returns.to_numpy() / 100) - 1
    ) * 100


def capture_ratio(subset):
    if subset.empty:
        return np.nan

    portfolio_compound = compounded_return_percent(
        subset["portfolio_return"]
    )
    index_compound = compounded_return_percent(
        subset["total_index_return"]
    )

    if np.isclose(index_compound, 0):
        return np.nan

    return portfolio_compound / index_compound * 100


up_days = data[data["total_index_return"] > 0]
down_days = data[data["total_index_return"] < 0]

upside_capture = capture_ratio(up_days)
downside_capture = capture_ratio(down_days)

print(f"Upside Capture: {upside_capture:.2f}%")
print(f"Downside Capture: {downside_capture:.2f}%")



import statsmodels.api as sm

def estimate_beta(subset):
    if len(subset) < 3:
        return np.nan, np.nan

    X = sm.add_constant(subset["total_index_return"])
    y = subset["portfolio_return"]

    model = sm.OLS(y, X).fit()

    return (
        model.params["total_index_return"],
        model.params["const"],
    )


up_beta, up_alpha = estimate_beta(up_days)
down_beta, down_alpha = estimate_beta(down_days)

print(f"Up Market Beta:   {up_beta:.3f}")
print(f"Down Market Beta: {down_beta:.3f}")
print(f"Up Intercept:     {up_alpha:.3f}")
print(f"Down Intercept:   {down_alpha:.3f}")





data["market_decile"] = pd.qcut(
    data["total_index_return"],
    q=10,
    labels=False,
    duplicates="drop",
) + 1

decile_stats = (
    data.groupby("market_decile")
    .agg(
        n_days=("excess_return", "size"),
        avg_market=("total_index_return", "mean"),
        avg_portfolio=("portfolio_return", "mean"),
        avg_excess=("excess_return", "mean"),
        win_rate=("outperformed", "mean"),
    )
)

decile_stats["win_rate"] *= 100

print(decile_stats.round(3))

