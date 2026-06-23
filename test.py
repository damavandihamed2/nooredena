import pandas as pd
from utils.database import make_connection

db_conn = make_connection()
portfolio = pd.read_sql("select p.date, p.portfolio_id, p.symbol, p.amount volume, p.total_cost, p.total_cost_sep, "
                        "pdr.close_price_y final_price from [nooredenadb].[portfolio].[portfolio] p LEFT JOIN "
                        "[nooredenadb].[portfolio].[portfolio_daily_report_diff] pdr ON "
                        "p.portfolio_id = pdr.portfolio_id_y and p.symbol = pdr.symbol_y", db_conn)
portfolio.to_excel("c:/users/h.damavandi/desktop/portfolio.xlsx", index=False)
