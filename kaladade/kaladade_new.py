import pandas as pd

from kaladade import kaladade
from utils.database import make_connection, insert_to_database


def update_categories(agent: kaladade.KaladadeAgent, db_conn) -> None:
    db_cursor = db_conn.cursor()
    agent.get_categories()
    categories_df = pd.DataFrame(agent.categories_data)
    subcategories_df = categories_df[["Children"]].explode("Children", ignore_index=True)
    subcategories_df = pd.json_normalize(subcategories_df["Children"])

    categories_df = categories_df[['CategoryId', 'CategoryName', 'CategoryType', 'Priority']]
    categories_df.rename({'CategoryId': "id", 'CategoryName': "name", 'CategoryType': "type",
                          'Priority': "priority"}, axis=1, inplace=True)
    subcategories_df = subcategories_df[['CategoryId', 'CategoryName', 'CategoryType', 'Priority', 'ParentId']]
    subcategories_df.rename({'CategoryId': 'id', 'CategoryName': 'name', 'CategoryType': 'type',
                             'Priority': 'priority', 'ParentId': 'category_id'}, axis=1, inplace=True)

    db_cursor.execute("TRUNCATE TABLE [nooredenadb].[kaladade].[categories]; "
                      "TRUNCATE TABLE [nooredenadb].[kaladade].[sub_categories];")
    insert_to_database(dataframe=categories_df, database_table="[nooredenadb].[kaladade].[categories]")
    insert_to_database(dataframe=subcategories_df, database_table="[nooredenadb].[kaladade].[sub_categories]")
    db_cursor.close()

def update_assets(agent: kaladade.KaladadeAgent, db_conn) -> None:
    sub_categories = pd.read_sql("SELECT * FROM [nooredenadb].[kaladade].[sub_categories]", db_conn)
    db_cursor = db_conn.cursor()
    # market_watch_list = []
    efc_internal_list = []
    efc_foreign_list = []
    for i in range(len(sub_categories)):

        id1, id2 = int(sub_categories["category_id"][i]), int(sub_categories["id"][i])
        print(f"(id1: {id1}) - (id2: {id2})")

        # kaladade_agent.get_market_watch(id1=id1, id2=id2)  # ime and iee data
        # market_watch = kaladade_agent.market_watch_data["Table"]["Rows"]
        # market_watch_list += market_watch

        agent.get_efc(id1=id1, id2=id2)
        efc = agent.efc_data

        efc_internal = efc["Internal"]["Rows"]
        efc_internal_list += efc_internal

        efc_foreign = efc["Foreign"]["Rows"]
        efc_foreign_list += efc_foreign

    # market_watch_df = pd.DataFrame(market_watch_list)
    efc_internal_df = pd.DataFrame(efc_internal_list)
    efc_foreign_df = pd.DataFrame(efc_foreign_list)
    efc = pd.concat([efc_internal_df, efc_foreign_df], axis=0, ignore_index=True)
    efc.drop(labels=['tdl', 'tdp', 'twl', 'twp', 'tml', 'tmp', 'tyl', 'typ', 'tzl', 'tzp'], axis=1, inplace=True)
    efc.replace({"ila": {True: 1, False: 0}}, inplace=True)
    efc.rename({"title": "name"}, axis=1, inplace=True)
    db_cursor.execute("TRUNCATE TABLE [nooredenadb].[kaladade].[assets]")
    db_cursor.close()
    insert_to_database(dataframe=efc, database_table="[nooredenadb].[kaladade].[assets]")

def update_currencies(agent: kaladade.KaladadeAgent, db_conn) -> None:
    agent.get_currencies()
    currencies = kaladade_agent.currencies_data
    currencies_df = pd.DataFrame(currencies["Currencies"])
    currencies_df.rename({"DateCode": "code", "EndDate_Jalali": "end_date_jalali", "CurrencyId": "id",
                          "CurrencyName_En": "name_en", "CurrencyName_Fa": "name_fa", "Price": "price",
                          "ModifiedAt": "modified_at", "TimeElapsed": "time_elapsed", "PriceIncrease": "price_increase",
                          "Ticker": "ticker"}, axis=1, inplace=True)
    db_cursor = db_conn.cursor()
    db_cursor.execute("TRUNCATE TABLE [nooredenadb].[kaladade].[currencies];")
    db_conn.close()
    insert_to_database(dataframe=currencies_df, database_table="[nooredenadb].[kaladade].[currencies]")

if __name__ == '__main__':

    db_connection = make_connection()

    phone_number = "09372377126"
    kaladade_agent = kaladade.KaladadeAgent()
    kaladade_agent.login(phone_number=phone_number)

    update_categories(agent=kaladade_agent, db_conn=db_connection)
    update_assets(agent=kaladade_agent, db_conn=db_connection)
    update_currencies(agent=kaladade_agent, db_conn=db_connection)

