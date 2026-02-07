import requests as rq
from time import sleep
import json, warnings, jdatetime

from utils import captcha_handler


warnings.filterwarnings("ignore")



class BrokersTadbirpardaz:
    def __init__(self, url, username, password):
        self.url_ = url
        self.user_ = username
        self.pass_ = password

        self.login_page_url = url + "User/Login"
        self.login_page_response = None
        self.captcha_guid = None
        self.asp_net_session_id = None

        self.captcha_url = url + "captcha.axd/"
        self.captcha_id = None
        self.captcha_response = None
        self.captcha_content = None
        self.captcha_refresh_url = url + "User/RefreshCaptcha"
        self.captcha_refresh_response = None

        self.captcha_value = None
        self.account_url = self.url_ + "User/Login"
        self.loginResponseStatus = None
        self.tbscptoken = None

        self.account_info_url = url + "Customers/AccountInfo"
        self.account_info_payload = '{"tradeSystem": 26}'
        self.account_info = None

        self.assets_url = url + "Customers/PortfoliosAjaxRead?page=1&start=0&limit=10000"
        self.assets = None
        self.assets_value = None

        self.trades_url = url + "TradeSummary/AjaxRead?MarketInstrument.Id=&Filter.TradeSide=Both&" \
                                "Filter.ReportType=Simple&page=1&start=0&limit=2147483647"
        self.trades_start_date = None
        self.trades_end_date = None
        self.trades = None

        self.ledger_url = url + 'Customers/AccountHistoryAjaxRead?Filter.TradeSystemEnum=TSETradingSystem&start=0&' \
                                'Filter.Type=Simple&sort=[{"property":"TransactionDate","direction":"ASC"}]&page=1&' \
                                'limit=1000000'
        self.ledger_start_date = None
        self.ledger_end_date = None
        self.ledger = None

    def login_page(self):
        self.login_page_response = rq.get(url=self.login_page_url)
        self.captcha_guid = self.login_page_response.text.split("/captcha.axd/")[1].split('"')[0]
        self.asp_net_session_id = self.login_page_response.headers["Set-Cookie"].split(
            "ASP.NET_SessionId=")[1].split(";")[0]

    def captcha(self):
        if self.captcha_guid is None:
            self.login_page()
            sleep(3)
        captcha_headers = {
            "accept-encoding": "gzip, deflate, br, zstd", "connection": "keep-alive", "sec-fetch-mode": "no-cors",
            "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8", "sec-ch-ua-mobile": "?0",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "referer": self.url_ + "User/Login", "accept-language": "en-US,en;q=0.9", "sec-fetch-dest": "image",
            "host": f"{self.url_.replace('https:', '').replace('/', '')}", "sec-ch-ua-platform": '"Windows"',
            "cookie": f"ASP.NET_SessionId={self.asp_net_session_id}; LocalizerCulture=fa-ir;",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/129.0.0.0 Safari/537.36", "sec-fetch-site": "same-origin"}
        sleep(3)
        self.captcha_refresh_response = rq.get(
            url=self.captcha_refresh_url + f'?{{"captcha-guid":"{self.captcha_guid}"}}', headers=captcha_headers)
        sleep(3)

        if self.captcha_refresh_response.status_code == 200:
            self.captcha_guid = self.captcha_refresh_response.json()["result"]["Key"]
            self.captcha_response = rq.get(url=self.captcha_url + f"{self.captcha_guid}", headers=captcha_headers)
            self.captcha_id = str(jdatetime.datetime.now().timestamp())
            if self.captcha_response.status_code == 200:
                self.captcha_content = self.captcha_response.content
                captcha_handler.save_captcha(captcha_type="tadbirpardaz", captcha_image=self.captcha_content,
                                             captcha_id=self.captcha_id, b64decode=False)
            else:
                print("Loading captcha failed!")

    def login(self):
        self.captcha()
        self.captcha_value = captcha_handler.open_captcha(captcha_type="tadbirpardaz", captcha_id=self.captcha_id)
        account_payload = f"Username={self.user_}&Password={self.pass_}&ReturnUrl=&LoginCaptcha={self.captcha_value}&" \
                          f"captcha-guid={self.captcha_guid}"
        account_header = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                          'Cookie': f'ASP.NET_SessionId={self.asp_net_session_id}',
                          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                                        ' (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'}
        self.login_response = rq.post(url=self.account_url, headers=account_header, data=account_payload)
        if self.login_response.status_code == 200:
            account_response_json = self.login_response.json()
            account_response_json_script = json.loads(account_response_json["script"].split(
                "show(")[1].split(")")[0].replace('Ext.Msg.OK', '"Ext.Msg.OK"').replace(
                'Ext.Msg.INFO', '"Ext.Msg.INFO"').replace('Ext.Msg.ERROR', '"Ext.Msg.ERROR"'))
            if account_response_json["success"]:
                if account_response_json_script["title"] == "ورود":
                    self.loginResponseStatus = 200
                    print("login success")
                if account_response_json_script["title"] == "Info":
                    self.loginResponseStatus = 200
                    print("login success but someone is already logged in")
                self.tbscptoken = self.login_response.headers["Set-Cookie"].split("TBSCPToken=")[1].split(";")[0]
                try:
                    captcha_handler.update_captcha_value(captcha_type="tadbirpardaz", captcha_id=self.captcha_id,
                                                         captcha_value=self.captcha_value)
                except Exception as e:
                    print(e)
            else:
                if "کد امنیتی" in account_response_json_script["msg"]:
                    self.loginResponseStatus = 401
                    print("Incorrect Captcha")
                if "نام" in account_response_json_script["msg"]:
                    self.loginResponseStatus = 401
                    print("Incorrect Username or Password")
                self.captcha_guid = account_response_json["result"]["Key"]
        else:
            self.loginResponseStatus = self.login_response.status_code

    def get_account_info(self):
        if self.loginResponseStatus != 200:
            self.login()
        else:
            pass
        account_info_headers = {
            'Referer': self.url_, 'Content-Type': 'application/json',
            'Cookie': f'ASP.NET_SessionId={self.asp_net_session_id}; TBSCPToken={self.tbscptoken};',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/129.0.0.0 Safari/537.36'}
        try:
            account_info_response = rq.post(url=self.account_info_url, headers=account_info_headers,
                                            data=self.account_info_payload)
            if account_info_response.status_code == 200:
                try:
                    self.account_info = account_info_response.json()
                except Exception as e:
                    raise Exception("Parsing account_info response failed - ", e)
            else:
                raise Exception('Getting account_info <RESPONSE> failed - status code: ',
                                account_info_response.status_code)
        except:
            raise Exception('Getting account_info <REQUEST> failed.')

    def get_assets(self):
        if self.loginResponseStatus != 200:
            self.login()
        else:
            pass
        portfolio_url = self.assets_url
        portfolio_headers = {"cookie": f"ASP.NET_SessionId={self.asp_net_session_id}; TBSCPToken={self.tbscptoken}",
                             "host": self.url_.split("/")[-2],
                             "referer": self.url_ + "Customers/Portfolios",
                             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, "
                                           "like Gecko) Chrome/129.0.0.0 Safari/537.36"}
        try:
            portfolio_response = rq.get(url=portfolio_url, headers=portfolio_headers)
            if portfolio_response.status_code == 200:
                try:
                    portfolio_response = portfolio_response.json()
                    self.assets = portfolio_response["data"]
                    self.assets_value = sum([asset["MomentaryValue"] for asset in self.assets])
                except Exception as e:
                    raise Exception("Parsing portfolio response failed - ", e)
            else:
                raise Exception('Getting portfolio <RESPONSE> failed - status code: ', portfolio_response.status_code)
        except:
            raise Exception('Getting portfolio <REQUEST> failed.')

    def get_trades(self, from_date, to_date):
        self.trades_start_date = from_date
        self.trades_end_date = to_date
        frmdte = jdatetime.datetime.strptime(self.trades_start_date, "%Y/%m/%d").togregorian().strftime("%m/%d/%Y")
        todte = jdatetime.datetime.strptime(self.trades_end_date, "%Y/%m/%d").togregorian().strftime("%m/%d/%Y")
        if self.loginResponseStatus != 200:
            self.login()
        else:
            pass
        trades_url = self.trades_url + f"&Filter.FromDate={frmdte}&Filter.ToDate={todte}"
        trades_headers = {"cookie": f"ASP.NET_SessionId={self.asp_net_session_id}; TBSCPToken={self.tbscptoken}",
                          "host": self.url_.split("/")[-2], "referer": self.url_ + "Customers/Portfolios",
                          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"}
        try:
            trades_response = rq.get(url=trades_url, headers=trades_headers)
            if trades_response.status_code == 200:
                try:
                    trades_response_json = trades_response.json()
                    self.trades = trades_response_json["data"]
                except Exception as e:
                    raise Exception("Parsing trades response failed - ", e)
            else:
                raise Exception('Getting trades <RESPONSE> failed - status code: ', trades_response.status_code)
        except:
            raise Exception('Getting trades <REQUEST> failed.')

    def get_ledger(self, from_date, to_date):
        self.ledger_start_date = from_date
        self.ledger_end_date = to_date
        frmdte = jdatetime.datetime.strptime(self.ledger_start_date, "%Y/%m/%d").togregorian().strftime("%m/%d/%Y")
        todte = jdatetime.datetime.strptime(self.ledger_end_date, "%Y/%m/%d").togregorian().strftime("%m/%d/%Y")
        if self.loginResponseStatus != 200:
            self.login()
        else:
            pass
        ledger_url = self.ledger_url + f'&Filter.DateFilter.StartDate={frmdte}&Filter.DateFilter.EndDate={todte}'
        ledger_headers = {"cookie": f"ASP.NET_SessionId={self.asp_net_session_id}; TBSCPToken={self.tbscptoken}",
                          "host": self.url_.split("/")[-2], "referer": self.url_ + "Customers/Portfolios",
                          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"}
        try:
            ledger_response = rq.get(url=ledger_url, headers=ledger_headers)
            if ledger_response.status_code == 200:
                try:
                    ledger_response_json = ledger_response.json()
                    self.ledger = ledger_response_json["data"]
                except Exception as e:
                    raise Exception("Parsing ledger response failed - ", e)
            else:
                raise Exception('Getting ledger <RESPONSE> failed - status code: ', ledger_response.status_code)
        except:
            raise Exception('Getting ledger <REQUEST> failed.')

