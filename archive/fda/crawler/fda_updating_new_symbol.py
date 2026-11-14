import pandas as pd
from pyodbc import Connection

from utils.database import (
    make_connection,
    insert_to_database
)



def update_new_symbol(symbol: str, db_connection: Connection):

    new_symbol_data = pd.read_sql(
        f"SELECT * FROM [nooredenadb].[fda].[medicines_weight] WHERE symbol='{symbol}'",
        db_connection
    )

    if new_symbol_data.empty:
        raise ValueError(
            f"there is no link for '{symbol}' in medicines_weight table"
        )
    new_symbol_data["medicine_id"] = new_symbol_data["link"].str[34:].astype(int)

    new_symbol_history = pd.read_sql(
        f"SELECT * FROM [nooredenadb].[fda].[medicines_history] where medicine_id IN "
        f"{str(tuple(new_symbol_data["medicine_id"].values.tolist()))} ORDER BY date",
        db_connection
    )
    selected_symbols_dates = pd.read_sql(
        "SELECT distinct(date) FROM [nooredenadb].[fda].[medicines_history_selected] ORDER BY date",
        db_connection
    )

    grid = selected_symbols_dates.assign(
        key=1).merge(
        pd.DataFrame({"medicine_id": new_symbol_history["medicine_id"].unique(), "key": 1}),
        on="key").drop(
        "key",
        axis=1)

    grid = grid.merge(
        new_symbol_history,
        on=["date", "medicine_id"],
        how="outer"
    ).sort_values(
        ["medicine_id", "date"],
        inplace=False,
        ascending=True,
        ignore_index=True
    )

    grid[['consumer_package_price', 'price']] = grid.groupby(
        by="medicine_id"
    )[['consumer_package_price', 'price']].ffill()
    grid.dropna(
        subset="price", inplace=True, ignore_index=True
    )
    grid = grid[
        grid["date"].isin(
            selected_symbols_dates["date"].values.tolist()
        )
    ].reset_index(
        drop=True,
        inplace=False
    )

    insert_to_database(dataframe=grid, database_table="[nooredenadb].[fda].[medicines_history_selected]")


def run(symbol: str):

    db_connection = make_connection()
    if db_connection:
        try:
            update_new_symbol(symbol=symbol, db_connection=db_connection)
            db_connection.close()
        finally:
            if db_connection:
                db_connection.close()


def main(symbol: str):
    run(
        symbol=symbol
    )
