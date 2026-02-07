import pandas as pd
from utils.database import make_connection
from dataclasses import dataclass

db_conn = make_connection()


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

def get_last_date(date_: str) -> str:
    query = "SELECT MAX(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE MWeekDay NOT IN " \
            "('Thursday', 'Friday') AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') FROM " \
            f"[nooredenadb].[extra].[closed_days]) AND Jalali_1 < '{date_}'"
    d_ = pd.read_sql(query, db_conn)["date"].iloc[0]
    return d_

def get_next_date(date_: str) -> str:
    query = "SELECT MIN(Jalali_1) AS date FROM [nooredenadb].[extra].[dim_date] WHERE " \
            "MWeekDay != 'Friday' AND Jalali_1 NOT IN (SELECT REPLACE([date], '-', '/') " \
            f"FROM [nooredenadb].[extra].[closed_days]) AND Jalali_1 > '{date_}'"
    d_ = pd.read_sql(query, db_conn)["date"].iloc[0]
    return d_

def capital_increase(params: CapChangeParams) -> dict[str, int]:
    new_share = params.new_share
    bonus = int((params.amount * (params.reserve / params.old_share)) // 1)
    ros = int((params.amount * (params.contribution / params.old_share)) // 1)
    cost = (params.total_cost + (1000 * ros)) / (params.amount + bonus + ros)
    cost_share = round(cost * params.amount)
    cost_bonus = round(cost * bonus)
    cost_ros = params.total_cost - (cost_share + cost_bonus)
    old_cap = params.old_share * params.price
    if params.premium == 0:
        adj_price = int(((old_cap + (1000 * params.contribution)) / new_share) // 1)
    else:
        adj_price = ((old_cap + (1000 * (params.contribution + params.premium + params.reserve))) / new_share) // 1
    return {
        "adj_price": adj_price,
        "ros_price": adj_price - 1000,
        "bonus": bonus,
        "ros": ros,
        "cost_share": cost_share,
        "cost_bonus": cost_bonus,
        "cost_ros": cost_ros,
    }

