import jdatetime
import pandas as pd
from pyodbc import Connection

from sigma.crawler.fetch import Agent
from utils.database import insert_to_database
from sigma.crawler.tools.update_tools import (
    cols_mapper_buysell,
    cols_mapper_profitloss,
    cols_mapper_portfolio,
    cols_mapper_dividend,
    dtype_mapper_buysell,
    get_last_buysell_date,
    get_last_profitloss_date,
    get_last_portfolio_date,
    get_end_portfolio_date,
    get_start_date,
    get_last_dividend_date,
    get_end_dividend_date,
)



def update_buysell_data(agent: Agent, portfolio_ids_list: list[int], db_connection: Connection):

    for portfolioId in portfolio_ids_list:

        last_buysell = get_last_buysell_date(portfolio_id=portfolioId, db_connection=db_connection)
        start_buysell = get_start_date(last_date=last_buysell)

        buysell_data = agent.get_buysell(
            start_date=start_buysell,
            end_date=jdatetime.datetime.now().strftime(format="%Y/%m/%d"),
            portfolio_id=portfolioId
        )

        if buysell_data:
            if len(buysell_data) > 0:
                buysell_df = pd.DataFrame(buysell_data)
                buysell_df = buysell_df[list(cols_mapper_buysell.keys())]
                buysell_df.rename(mapper=cols_mapper_buysell, axis=1, inplace=True)
                buysell_df = buysell_df.astype(dtype_mapper_buysell)
                buysell_df["portfolio_id"] = portfolioId
                insert_to_database(dataframe=buysell_df, database_table="[nooredenadb].[sigma].[buysell]")


def update_profitloss_data(agent: Agent, portfolio_ids_list: list[int], db_connection: Connection):

    for portfolioId in portfolio_ids_list:

        last_profitloss = get_last_profitloss_date(portfolio_id=portfolioId, db_connection=db_connection)
        start_profitloss = get_start_date(last_date=last_profitloss)
        profitloss_data = agent.get_profitloss(
            start_date=start_profitloss,
            end_date=jdatetime.datetime.now().strftime(format="%Y/%m/%d"),
            portfolio_id=portfolioId
        )

        if profitloss_data:
            if len(profitloss_data) > 0:
                profitloss_df = pd.DataFrame(profitloss_data)
                profitloss_df = profitloss_df[list(cols_mapper_profitloss.keys())]
                profitloss_df.rename(mapper=cols_mapper_profitloss, axis=1, inplace=True)
                profitloss_df["portfolio_id"] = portfolioId
                insert_to_database(dataframe=profitloss_df, database_table="[nooredenadb].[sigma].[profitloss]")


def update_portfolio_data(agent: Agent, portfolio_ids_list: list[int], db_connection: Connection):

    for portfolioId in portfolio_ids_list:

        last_portfolio = get_last_portfolio_date(portfolio_id=portfolioId, db_connection=db_connection)
        start_portfolio = get_start_date(last_date=last_portfolio)
        end_portfolio = get_end_portfolio_date(portfolio_id=portfolioId, db_connection=db_connection)

        if end_portfolio >= start_portfolio:
            portfolio = pd.DataFrame()
            date_ = start_portfolio
            while date_ <= end_portfolio:

                print(f"getting portfolio data for ({portfolioId=} - {date_=})")

                portfolio_data = agent.get_portfolio(date=date_, portfolio_id=portfolioId)

                if portfolio_data:
                    if len(portfolio_data) > 0:
                        portfolio_df = pd.DataFrame(portfolio_data)
                        portfolio_df["date"] = date_
                        portfolio = pd.concat([portfolio, portfolio_df], axis=0, ignore_index=True)

                date_ = (jdatetime.datetime.strptime(date_, format="%Y/%m/%d") + jdatetime.timedelta(days=1)).strftime(format="%Y/%m/%d")

            portfolio = portfolio[list(cols_mapper_portfolio.keys())]
            portfolio.rename(mapper=cols_mapper_portfolio, axis=1, inplace=True)
            portfolio["portfolio_id"] = portfolioId

            insert_to_database(dataframe=portfolio, database_table="[nooredenadb].[sigma].[portfolio]")


def update_dividend_data(agent: Agent, portfolio_ids_list: list[int], db_connection: Connection):

    for portfolioId in portfolio_ids_list:

        last_dividend = get_last_dividend_date(portfolio_id=portfolioId, db_connection=db_connection)
        if not last_dividend:
            last_dividend = "1400/01/01"
        end_dividend = get_end_dividend_date(portfolio_id=portfolioId, db_connection=db_connection)

        dividend_data = agent.get_dividend(start_date=last_dividend, end_date=end_dividend, portfolio_id=portfolioId)

        if dividend_data:
            dividend_df = pd.DataFrame(dividend_data)
            dividend_df = dividend_df[list(cols_mapper_dividend.keys())]
            dividend_df.rename(mapper=cols_mapper_dividend, axis=1, inplace=True)
            dividend_df["portfolio_id"] = portfolioId
            dividend_df = dividend_df[dividend_df["value"] > 0]
            dividend_df = dividend_df[dividend_df["meeting_date"] >= last_dividend].reset_index(drop=True, inplace=False)

            if len(dividend_df) > 0:
                crsr = db_conn.cursor()
                crsr.execute(f"DELETE FROM [nooredenadb].[sigma].[dividend] "
                             f"WHERE portfolio_id={portfolioId} AND meeting_date >= '{last_dividend}'")
                crsr.close()
                insert_to_database(dataframe=dividend_df, database_table="[nooredenadb].[sigma].[dividend]")


def update_all(agent: Agent, portfolio_ids_list: list[int], db_connection: Connection):

    update_buysell_data(agent=agent, portfolio_ids_list=portfolio_ids_list, db_connection=db_connection)
    update_profitloss_data(agent=agent, portfolio_ids_list=portfolio_ids_list, db_connection=db_connection)
    update_portfolio_data(agent=agent, portfolio_ids_list=portfolio_ids_list, db_connection=db_connection)
    update_dividend_data(agent=agent, portfolio_ids_list=portfolio_ids_list, db_connection=db_connection)



if __name__ == "__main__":

    from utils.database import make_connection

    db_conn = make_connection()

    if db_conn:

        authenticator = [{"username": "damavandi", "password": "Dd@123456"}]
        sigma_agent = Agent(
            username=authenticator[0]["username"],
            password=authenticator[0]["password"],
            db_connection=db_conn
        )
        sigma_agent.login()

        update_buysell_data(agent=sigma_agent, portfolio_ids_list=[1, 25], db_connection=db_conn)
        update_profitloss_data(agent=sigma_agent, portfolio_ids_list=[1, 25], db_connection=db_conn)
        update_portfolio_data(agent=sigma_agent, portfolio_ids_list=[1, 25], db_connection=db_conn)
        update_dividend_data(agent=sigma_agent, portfolio_ids_list=[1, 25], db_connection=db_conn)


