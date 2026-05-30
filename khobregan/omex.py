import uuid
import jdatetime
import requests as rq
import io, json, base64
import PIL.Image as pil
from math import ceil, floor
from datetime import datetime, timezone

from khobregan.env_manager import EnvManager


manager = EnvManager(path="D:/PythonProjects/nooredena/khobregan")


def calculate_volume(price: int, balance: int, commission: float) -> int:
    volume = floor(balance / (price * (1 + commission)))
    return volume


def get_api_address(address: str, api_app: str):
    api_address = api_app + "." + ".".join(address.rstrip("/").split(".")[-2:])
    if "khobregan" in address:
        api_address = "khobregan-" + api_address
    elif "mebbco" in address:
        api_address = "mebbco-" + api_address
    else:
        pass
    return api_address

class OmexAgent:
    def __init__(
            self, address: str,
            username: str,
            password: str
    ):
        self.auth_token_web_address = address.split("//")[1].replace("/", "")
        self.website_address = address
        self.username = username
        self.password = password
        self._user_is_login = False

        self.base_headers = {'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
                             'Cache-Control': 'no-cache', 'sec-ch-ua-platform': '"Windows"', 'ngsw-bypass': '',
                             'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-site',
                             'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
                             'Pragma': 'no-cache', 'sec-ch-ua-mobile': '?0', 'Connection': 'keep-alive',
                             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                           '(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                             'Origin':  f'https://{self.website_address}',
                             'Referer': f'https://{self.website_address}/'}
        self.captcha_url = f"https://{get_api_address(self.website_address, "white")}/api/Captcha/get"
        self.get_profile_url = f'https://{get_api_address(self.website_address, "delta")}/api/Customers/GetProfile'
        self.login_url = f"https://{get_api_address(self.website_address, "white")}/api/Accounts/Login"
        self.logout_url = f'https://{get_api_address(self.website_address, "white")}/api/Accounts/Logout'
        self.order_entry_url = f"https://{get_api_address(self.website_address, "red")}/api/Orders/OrderEntry"
        self.balance_url = f"https://{get_api_address(self.website_address, "red")}/api/Customers/wallet-info"
        self.get_instrument_info_url = f"https://{get_api_address(self.website_address, "red")}/api/PublicMessages/GetInstruments"
        self.get_time_diff_url = f"https://{get_api_address(self.website_address, "red")}/api/PublicMessages/TimeDiff"
        self.get_portfolio_url = f"https://{get_api_address(self.website_address, "delta")}/api/assets/portfolio-info"


        self.captcha_headers = self.base_headers
        self.captcha_response = self.captcha_id =self.captcha_token = self.captcha_image = self.captcha_value = None

        self.login_headers = self.base_headers
        self.login_payload = self.loginResponseStatus = self.login_response = None

        self.logout_headers = self.logout_payload = self.logout_response = None

        self.access_token = self.headers = self.cookies = None

        self.portfolio = None
        self.balance = None
        self.instrument_info = None

        self.order_entry_response = None
        self.order_entry_list = []

    ################################################################

    def _get_old_token(self) -> tuple:
        token = manager.get_token("TOKEN")
        if token:
            try:
                token = json.loads(token)
                acc_token, cookies = token["access_token"], token["cookies"]
                return acc_token, cookies
            except Exception:
                pass
        return "", {}


    def _update_token(self, access_token: str, cookies: dict) -> None:
        json_data = {"access_token": access_token, "cookies": cookies}
        manager.update_or_add("TOKEN", json.dumps(json_data))


    def check_old_tokens(self):
        access_token_, cookies_ = self._get_old_token()
        check_token_headers = {**self.base_headers, "Authorization": access_token_}
        try:
            check_token_response = rq.get(
                url=self.get_profile_url, headers=check_token_headers, cookies=cookies_)
            if check_token_response.status_code == 200:
                self._user_is_login = True
                self.loginResponseStatus = 200
                self.access_token = access_token_
                self.cookies = cookies_
                self.headers = {**self.base_headers, "Authorization": self.access_token}
            else:
                self._user_is_login = False
        except Exception as e:
            raise e

    ################################################################

    def get_captcha(self):
        self.captcha_id = str(jdatetime.datetime.now().timestamp())
        try:
            self.captcha_response = rq.get(
                url=self.captcha_url,
                headers=self.captcha_headers
            )
            if self.captcha_response.status_code == 200:
                captcha_successful = self.captcha_response.json()["response"]["successful"]
                if captcha_successful:
                    captcha_data = self.captcha_response.json()["response"]["data"]
                    self.captcha_token = captcha_data["token"]
                    self.captcha_image = base64.b64decode(captcha_data["image"])
        except Exception as e:
            raise e


    def login(self):

        self.check_old_tokens()

        if self._user_is_login:
            pass

        else:

            self.get_captcha()

            img = pil.open(io.BytesIO(self.captcha_image))
            img.resize(size=(img.size[0] * 3, img.size[1] * 3)).show()
            self.captcha_value = input("Please Enter The Captcha Phrase: ")
            img.close()

            self.login_payload = {
                "username": self.username,
                "password": self.password,
                "captchaToken": self.captcha_token,
                "captchaResponseCode": self.captcha_value
            }
            try:
                self.login_response = rq.post(
                    url=self.login_url,
                    headers=self.login_headers,
                    json=self.login_payload
                )
                if self.login_response.status_code == 200:
                    login_response_successful = self.login_response.json()["response"]["successful"]
                    if login_response_successful:
                        self.access_token = self.login_response.json()["response"]["data"]["tokenInfo"]["token"]
                        self.cookies = self.login_response.cookies.get_dict()
                        self.headers = {**self.base_headers, "Authorization": self.access_token}
                        self.loginResponseStatus = 200
                        self._user_is_login = True
                        self._update_token(access_token=self.access_token, cookies=self.cookies)

                if self.login_response.status_code == 600:
                    self.loginResponseStatus = 600
                    self._user_is_login = False
                    if self.login_response.json()["response"]["errors"][0]["code"] == "CaptchaCodeIsInvalid":
                        raise UserWarning("Captcha is invalid.")
                    if self.login_response.json()["response"]["errors"][0]["code"] == "LoginException":
                        raise UserWarning("Username or Password is incorrect.")
            except Exception as e:
                raise UserWarning(f"Failed to login - {e}")

    def logout(self):
        if not self._user_is_login:
            raise UserWarning("You are not logged in!")
        else:
            try:
                self.logout_response = rq.post(
                    url=self.logout_url,
                    headers=self.headers,
                    cookies=self.cookies,
                    json={}
                )
                self._user_is_login = False
                print("You logged out successfully")
            except Exception as e:
                raise rq.HTTPError(f"Failed to logout - {e}")

    def _get_time_diff(self):
        response = rq.get(
            url=self.get_time_diff_url + f'?dt={datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')}',
            cookies=self.cookies, headers=self.headers)
        if response.status_code == 200:
            if response.json()["response"]["successful"]:
                self.time_now = response.json()["response"]["data"]["now_Utc"]

    def _get_balance(self):
        response = rq.get(self.balance_url, cookies=self.cookies, headers=self.headers)
        if response.status_code == 200:
            if response.json()["response"]["successful"]:
                self.balance = response.json()["response"]["data"][0]

    def _get_portfolio(self):
        response = rq.get(self.get_portfolio_url, cookies=self.cookies, headers=self.headers)
        if response.status_code == 200:
            if response.json()["response"]["successful"]:
                self.portfolio = response.json()["response"]["data"]["items"]

    def _get_instrument_info(self, instrument_id: str):
        response = rq.post(url=self.get_instrument_info_url, cookies=self.cookies,
                           headers=self.headers, json={'instrumentIds': [instrument_id]})
        if response.status_code == 200:
            if response.json()["response"]["successful"]:
                self.instrument_info = response.json()["response"]["data"][0]


    def _order_entry(self, instrument_id: str, side: str, price: int, volume: int):
        json_data = {'PrincipalId': None, 'InstrumentId': instrument_id, 'ISensOM': side, 'YValiOmNSC': 'Day',
                     'DValiOM': None, 'PLimSaiOM': price, 'QTitTotOM': volume, 'QTitDvlOM': 0,
                     'extraInfo': f'{{"ark":"{uuid.uuid4()}"}}', 'ExecutionType': 'Instant'}
        response = rq.post(url=self.order_entry_url, headers=self.headers, cookies=self.cookies, json=json_data)
        return response


    def _order_input_handler(self, instrument_id: str, side: int, price: int, volume: int | None):

        side = "Buy" if side == 1 else "Sell" if side == 2 else None

        self._get_instrument_info(instrument_id)
        if not volume:
            self._get_balance()
            volume = calculate_volume(price=price, balance=self.balance["remainedCash"], commission=0.003712)
        else:
            if side is "Buy":
                if self.balance is None:
                    self._get_balance()
                if ceil(volume * price * 1.003712) > self.balance["remainedCash"]:
                    raise ValueError(f"You don't have enough money! you need {ceil(volume * price * 1.003712)} IRR "
                                     f"but you have {self.balance["remainedCash"]} IRR")
        {"price": price, "volume": volume, "": "", "": "", "": "", "": "", "": "", "": "", "": "", }


    def order(self, instrument_id: str, side: int, price: int, volume: int | None):
        if not self._user_is_login:
            raise ValueError("You are not logged in, Login First!")
        side = "Buy" if side == 1 else "Sell" if side == 2 else None
        self._get_instrument_info(instrument_id)
        if not volume:
            self._get_balance()
            volume = calculate_volume(price=price, balance=self.balance["remainedCash"], commission=0.003712)
        else:
            if side is "Buy":
                if self.balance is None:
                    self._get_balance()
                if ceil(volume * price * 1.003712) > self.balance["remainedCash"]:
                    raise ValueError(f"You don't have enough money! you need {ceil(volume * price * 1.003712)} IRR "
                                     f"but you have {self.balance["remainedCash"]} IRR")

        self._order_entry(instrument_id, side, price, volume)







