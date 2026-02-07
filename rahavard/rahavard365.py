import json
import warnings
import pandas as pd
import requests as rq


warnings.filterwarnings("ignore")


class Agent:
    def __init__(self, username, password):

        self.base_url = "https://rahavard365.com/api/v2"

        self.username = username
        self.password = password

        self.login_status = None
        self.session_id = None
        self.access_token = None
        self.account = None

        if self.login_status != 200:
            self.login()
        else:
            pass

    def login(self):
        header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/126.0.0.0 Safari/537.36', 'content-type': 'application/json'}
        response = rq.post(url=f"{self.base_url}/sessions",
                           data=json.dumps({"username": self.username, "password": self.password}), headers=header)
        if response.status_code == 200:
            self.login_status = 200
            try:
                response = response.json()
                self.session_id = response["data"]["session_id"]
                self.access_token = response["data"]["access_token"]
                self.account = response["data"]["account"]
            except Exception as e:
                print(e)
        else:
            print("Login Failed (username or password is wrong)")


class Asset:
    def __init__(self, asset_id: str, agent: Agent):

        self.session_id = None
        self.access_token = None
        self.account = None

        self.idx_ = asset_id
        self.__dict__.update(agent.__dict__)
        self.header = {'authorization': f'Bearer {self.access_token}', 'content-type': 'application/json',
                       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Chrome/126.0.0.0 Safari/537.36'}

        self.get_asset_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}"
        self.asset = None

        self.get_capital_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/capital"
        self.capital_changes = None
        self.max_new_capital = None

        self.get_posts_url = f"https://rahavard365.com/api/v2/social/posts?login_account_id={self.account['id']}&" \
                             f"entity_id={self.idx_}&entity_type=exchange.exchange"
        self.posts = pd.DataFrame()

        self.get_eps_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/eps"
        self.eps = None

        self.get_dps_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/dps"
        self.dps = None

        self.get_fund_dps_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/funddividendpayment"
        self.fund_dps = None

        self.get_indicators_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/indicators"
        self.indicators = None

        self.get_feed_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/feeds"
        self.feed = None

        self.get_report_url = f"https://rahavard365.com/api/v2/report/reports?asset_id={self.idx_}"
        self.report = None

        self.get_production_sale_url = f"https://rahavard365.com/api/v2/asset/{self.idx_}/productionsale"
        self.production_sale = None

    def get_asset(self):
        response = rq.get(self.get_asset_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                self.asset = response["data"]
            except Exception as e:
                print(e)
        else:
            pass

    def get_capital(self):
        response = rq.get(self.get_capital_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                self.max_new_capital = response["data"]["max_new_capital"]
                self.capital_changes = pd.DataFrame(response["data"]["capital_changes"])
            except Exception as e:
                print(e)
        else:
            pass

    def get_posts(self, last_id: int = None, exclude_replies: bool = True, has_chart_analysis: bool = True,
                  count: int = 10):
        if last_id is None:
            url = self.get_posts_url + f"exclude_replies={str(exclude_replies).lower()}&" \
                                       f"has_chart_analysis={str(has_chart_analysis).lower()}&_count={count}"
        else:
            url = self.get_posts_url + f"exclude_replies={str(exclude_replies).lower()}&before_id={last_id}&" \
                                       f"has_chart_analysis={str(has_chart_analysis).lower()}&_count={count}&"
        response = rq.get(url=url, headers=self.header)
        if response.status_code == 200:
            posts = response.json()
            posts = pd.DataFrame(posts["data"])
            self.posts = pd.concat([self.posts, posts], axis=0, ignore_index=True)
        else:
            pass

    def get_eps(self, capital_last: bool = False):

        if capital_last:
            response = rq.get(url=self.get_eps_url + "?capital=last", headers=self.header)
        else:
            response = rq.get(url=self.get_eps_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                eps_df = pd.DataFrame(response["data"]["eps_fiscal_years"])
                if len(eps_df) > 0:
                    eps_df = pd.DataFrame([item for sublist in eps_df['epses'] for item in sublist])
                    eps_df.sort_values(by=["fiscal_year"], ignore_index=True, inplace=True, ascending=False)
                    self.eps = eps_df
            except Exception as e:
                print(e)
        else:
            pass

    def get_dps(self):
        response = rq.get(self.get_dps_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                dps_df = pd.DataFrame(response["data"])
                self.dps = dps_df
            except Exception as e:
                print(e)
        else:
            pass

    def get_fund_dps(self):
        response = rq.get(self.get_fund_dps_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                fund_dps_df = pd.DataFrame(response["data"])
                self.fund_dps = fund_dps_df
            except Exception as e:
                print(e)
        else:
            pass

    def get_indicators(self):
        response = rq.get(url=self.get_indicators_url, headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()
                self.indicators = response["data"]
            except Exception as e:
                print(e)
        else:
            pass

    def get_feed(self, count: int = 20):
        response = rq.get(url=self.get_feed_url + f"?_count={count}", headers=self.header)
        if response.status_code == 200:
            try:
                response = response.json()["data"]
                feed_df = pd.DataFrame(response)
                self.feed = feed_df
            except Exception as e:
                print(e)

    def get_report(self, count: int = 20, skip: int = 0):
        response = rq.get(url=self.get_report_url + f"&_skip={skip}&_count={count}", headers=self.header)
        if response.status_code == 200:
            try:
                report_df = pd.DataFrame(response.json()["data"])
                self.report = report_df
                pass
            except Exception as e:
                print(e)

    def get_production_sale(self, count: int = 10, skip: int = 0):
        response = rq.get(url=self.get_production_sale_url + f"?_skip={skip}&_count={count}", headers=self.header)
        if response.status_code == 200:
            try:
                production_sale_df = pd.DataFrame(response.json()["data"])
                if len(production_sale_df) > 0:
                    production_sale_df = pd.concat(
                        [production_sale_df, pd.DataFrame(production_sale_df["production_sale"].to_list())],
                        axis=1).drop(columns="production_sale", inplace=False)
                    self.production_sale = production_sale_df
                else:
                    pass
            except Exception as e:
                print(e)

if __name__ == '__main__':
    agent = Agent(username="09372377126", password="Dh74@123456")
    asset453 = Asset(asset_id="453", agent=agent)
    asset453.get_asset()
    asset453.get_capital()
    asset453.get_eps()
    asset453.get_dps()
    asset453.get_posts()
    asset453.get_indicators()
    asset453.get_feed()
    asset453.get_production_sale()
    asset453.get_report()

