import warnings

from annual_meeting.utils.data import get_credits, get_portfolio_data_daily


warnings.filterwarnings("ignore")

s_date = "1403/10/30"
e_date = "1404/10/30"

credits_df = get_credits(start_date=s_date, end_date=e_date)
portfolio_df = get_portfolio_data_daily(start_date=s_date, end_date=e_date)

portfolio_df = portfolio_df.merge(credits_df, how="left")

portfolio_df["remaining"].ffill(inplace=True)
portfolio_df["remaining_yesterday"] = portfolio_df["remaining"].shift(1)
portfolio_df["value_yesterday"] = portfolio_df["value"].shift(1)
portfolio_df["cost_yesterday"] = portfolio_df["cost"].shift(1)
portfolio_df.fillna({"dividend": 0, "profit": 0}, inplace=True)
portfolio_df["portfo_return"] = (portfolio_df["value"] + portfolio_df["dividend"] + portfolio_df["profit"] -
                                 portfolio_df["cost"] - (portfolio_df["value_yesterday"] -
                                                         portfolio_df["cost_yesterday"])) / portfolio_df["value_yesterday"]
portfolio_df["credit_profit"] = portfolio_df["portfo_return"] * portfolio_df["remaining_yesterday"]
portfolio_df.to_excel(f"./annual_meeting/portfolio_credits.xlsx", index=False)
