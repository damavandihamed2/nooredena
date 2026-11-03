from pyodbc import Connection

from fipiran.fetcher import get_funds_all, get_fund, get_fund_types
from fipiran.tools import get_reg_numbers_unique, remove_duplicates
from fipiran.update_db import upsert_funds_all, upsert_fund_types, upsert_funds, upsert_fund_ranks



def update_funds_all(db_connection: Connection):
    all_funds = get_funds_all()
    upsert_funds_all(funds_data=all_funds, db_connection=db_connection)


def update_fund_types(db_connection: Connection):
    fund_types = get_fund_types()
    upsert_fund_types(fund_types, db_connection=db_connection)


def update_funds(db_connection: Connection):

    reg_numbers = get_reg_numbers_unique(db_connection=db_connection)

    funds_list = []
    for rn in reg_numbers:
        gi = 0
        fund_data = get_fund(reg_number=rn, group_id=gi)
        if fund_data:
            funds_list.append(fund_data)
        while True:
            gi += 1
            fund_data = get_fund(reg_number=rn, group_id=gi)
            if fund_data:
                funds_list.append(fund_data)
            else:
                break

    funds_rank = [f.pop("fundRank") for f in funds_list if "fundRank" in f]

    funds_list = remove_duplicates(funds_list)
    upsert_funds(funds_data=funds_list, db_connection=db_connection)

    funds_rank = list({d["id"]: d for d in funds_rank}.values())
    upsert_fund_ranks(fund_ranks=funds_rank, db_connection=db_connection)
