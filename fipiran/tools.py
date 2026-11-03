import pandas as pd
from pyodbc import Connection



def get_reg_numbers_unique(db_connection: Connection) -> list[str]:
    reg_numbers = pd.read_sql(
        "SELECT distinct(regNo) FROM [nooredenadb].[fipiran].[funds_all]",
        db_connection
    )
    reg_numbers = reg_numbers["regNo"].values.tolist()
    return reg_numbers


def remove_duplicates(data: list[dict]) -> list[dict]:
    unique = {}

    for item in data:
        key = (item["regNo"], item["groupId"])

        if key not in unique:
            unique[key] = item
            continue

        existing = unique[key]

        if item.get("insCode") or not existing.get("insCode"):
            unique[key] = item

    return list(unique.values())
