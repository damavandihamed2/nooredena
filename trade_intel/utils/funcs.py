import jdatetime
import pandas as pd
from pyodbc import Connection
from dataclasses import dataclass



@dataclass(frozen=False)
class CapChangeParams:
    amount: int
    total_cost: int
    price: int
    old_share: int
    contribution: int
    premium: int
    reserve: int
    @property
    def new_share(self) -> int:
        return  self.old_share + self.contribution + self.premium + self.reserve


def relu(x: int) -> int:
    return max(0, x)


def get_last_date(date_: str, db_connection: Connection) -> str:
    query = "SELECT MAX(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE MWeekDay NOT IN " \
            "('Thursday', 'Friday') AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') FROM " \
            f"[nooredenadb].[extra].[closed_days]) AND Jalali_1 < '{date_}'"
    d_ = pd.read_sql(query, db_connection)["date"].iloc[0]
    return d_


def get_next_date(date_: str, db_connection: Connection) -> str:
    query = "SELECT MIN(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE " \
            "MWeekDay != 'Friday' AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') " \
            f"FROM [nooredenadb].[extra].[closed_days]) AND Jalali_1 > '{date_}'"
    d_ = pd.read_sql(query, db_connection)["date"].iloc[0]
    return d_


def capital_increase(params: CapChangeParams) -> dict[str, int]:
    new_share = params.new_share
    bonus = int((params.amount * (params.reserve / params.old_share)) // 1)
    ros = int((params.amount * (params.contribution / params.old_share)) // 1)
    cost = (params.total_cost + (1000 * ros)) / (params.amount + bonus + ros)
    cost_share = int(round(cost * params.amount))
    cost_bonus = int(round(cost * bonus))
    cost_ros = params.total_cost - (cost_share + cost_bonus)
    old_cap = params.old_share * params.price
    if params.premium == 0:
        adj_price = int(((old_cap + (1000 * params.contribution)) / new_share) // 1)
    else:
        adj_price = int(((old_cap + (1000 * (params.contribution + params.premium + params.reserve))) / new_share) // 1)
    return {
        "adj_price": adj_price,
        "ros_price": adj_price - 1000,
        "bonus": bonus,
        "ros": ros,
        "cost_share": cost_share,
        "cost_bonus": cost_bonus,
        "cost_ros": cost_ros,
    }


def get_fundamental_data(db_connection: Connection):

    dps_funds = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps_funds]", db_connection)
    dps_funds["date"] = dps_funds.apply(lambda row: jdatetime.datetime.fromgregorian(
        year=int(row["date"][:4]), month=int(row["date"][5:7]), day=int(row["date"][8:10])
    ).strftime("%Y/%m/%d") if row["date"] is not None else None, axis=1)

    dps = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[dps]", db_connection)
    for c in ['fiscal_year', 'date_time', 'announcement_date']:
        dps[c] = dps.apply(lambda row: jdatetime.datetime.fromgregorian(
            year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
        ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

    capital_changes = pd.read_sql("SELECT * FROM [nooredenadb].[rahavard].[capital_changes]", db_connection)
    for c in ['date', 'underwriting_end_date', 'registration_date', 'stock_certificate_receive_date', 'warrant_sell_date']:
        capital_changes[c] = capital_changes.apply(lambda row: jdatetime.datetime.fromgregorian(
            year=int(row[c][:4]), month=int(row[c][5:7]), day=int(row[c][8:10])
        ).strftime("%Y/%m/%d") if row[c] is not None else None, axis=1)

    return {
        "dps": dps,
        "dps_funds": dps_funds,
        "capital_changes": capital_changes,
    }

