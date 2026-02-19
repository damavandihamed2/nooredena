import requests as rq
import json, datetime, warnings

from utils import captcha_handler
from utils import auth_token_hadler
from utils.database import make_connection
from kaladade.kaladade_decoder import KaladadeDecoder


warnings.filterwarnings("ignore")
db_conn = make_connection()

#######################################################################################################################

class KaladadeAgent:

    api_address = "https://api.kaladade.ir/api"

    def __init__(self) -> None:
        self.default_headers = {'accept-language': 'fa-IR',
                                "content-type": "application/json",
                                'pragma': 'no-cache',
                                'cache-control': 'no-cache', 'sec-ch-ua-platform': '"Windows"', 'priority': 'u=1, i',
                                'origin': 'https://kaladade.ir', "accept-encoding": "gzip, deflate, br, zstd",
                                'referer': 'https://kaladade.ir/', 'accept': 'application/json, text/plain, */*',
                                'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                                'sec-ch-ua-mobile': '?0', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors',
                                'sec-fetch-site': 'same-site', "x-appversion": "0.0.3.72", "x-serial": "NA",
                                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                              "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"}

        self.init_url = f"{KaladadeAgent.api_address}/application/init"
        self.init_response = self.init_response_json = self.session_id = None

        self.check_token_url = f"{KaladadeAgent.api_address}/report/home/MarketWatch?id1=10004&id2=0"
        self.check_token_headers = self.check_token_response = None

        self.captcha_url = f"{KaladadeAgent.api_address}/Captcha"
        self.captcha_id = self.captcha_headers = self.captcha_response = self.captcha_response_json = None
        self.captcha_image = self.captcha_Id = self.captcha_value = None

        self.confirm_code_url = f"{KaladadeAgent.api_address}/Account/Phone/SendConfirmCode"
        self.confirm_code_payload = self.confirm_code_headers = None
        self.confirm_code_response = self.confirm_code_value = None

        self.login_url = f"{KaladadeAgent.api_address}/account/login"
        self.login_payload = self.login_headers = self.login_response = None

        self.access_token = None
        self.user_login_id = None

        self._get_data_url = self._get_data_headers = self._get_data_response = None
        self._get_data_response_json = self._data = None

        self.categories_url = f"{KaladadeAgent.api_address}/report/home/categories"
        self.categories_data = None

        self.market_watch_url = f"{KaladadeAgent.api_address}/report/home/MarketWatch"
        self.market_watch_data = None

        self.efc_url = f"{KaladadeAgent.api_address}/report/home/efc"
        self.efc_data = None

        self.currencies_url = f"{KaladadeAgent.api_address}/currencyboard"
        self.currencies_data = None


    def _get_old_token(self) -> [str, str, str]:
        token = auth_token_hadler.get_tokens(app="kaladade", web_address="kaladade")
        if token:
            try:
                access_token = token["access_token"]
                session_id = token["session_id"]
                user_login_id = token["user_login_id"]
                return access_token, session_id, user_login_id
            except Exception:
                pass
        return "", "", ""

    def _update_tokens(self, access_token: str, session_id: str, user_login_id: str) -> None:
        json_data = {"access_token": access_token, "session_id": session_id, "user_login_id": user_login_id}
        auth_token_hadler.update_tokens(app="kaladade", web_address="kaladade", json_data=json_data)

    def _check_old_tokens(self) -> bool:
        self.access_token, self.session_id, self.user_login_id = self._get_old_token()
        self.check_token_headers = {**self.default_headers, "x-uuid": self.session_id,
                                    "authorization": "Bearer " + self.access_token}

        self.check_token_response = rq.get(url=self.check_token_url, headers=self.check_token_headers)
        if self.check_token_response.status_code == 200: return True
        else: return False

    def _init(self) -> None:
        self.init_response = rq.get(url=self.init_url)
        self.init_response_json = self.init_response.json()
        self.session_id = self.init_response_json["Data"]["SessionId"]

    def _get_captcha(self) -> None:
        self.captcha_id = str(datetime.datetime.now().timestamp())
        self.captcha_headers = {**self.default_headers, "x-uuid": self.session_id}
        self.captcha_response = rq.get(url=self.captcha_url, headers=self.captcha_headers)
        self.captcha_response_json = self.captcha_response.json()["Data"]
        self.captcha_Id = self.captcha_response_json["Id"]
        self.captcha_image = self.captcha_response_json["Base64String"]

    def _get_confirmation_code(self, phone_number) -> None:
         self.confirm_code_payload = json.dumps({"Phone": phone_number})
         self.confirm_code_headers = {**self.default_headers, "x-uuid": self.session_id}
         self.confirm_code_response = rq.post(url=self.confirm_code_url,
                                              headers=self.confirm_code_headers,
                                              data= self.confirm_code_payload)
         self.confirm_code_value = input("Please Enter The Confirm Code:")

    def login(self, phone_number: str) -> None:
        is_old_token_valid = self._check_old_tokens()
        if is_old_token_valid:
            pass
        else:
            self._init()
            self._get_captcha()
            captcha_handler.save_captcha(
                captcha_type="kaladade", captcha_image=self.captcha_image, captcha_id=self.captcha_id)
            self.captcha_value = captcha_handler.open_captcha(captcha_type="kaladade", captcha_id=self.captcha_id)
            self._get_confirmation_code(phone_number=phone_number)
            self.login_payload = (f'{{"Phone": "{phone_number}", "CaptchaId": "{self.captcha_Id}", '
                                  f'"UserEnteredCaptchaCode": "{self.captcha_value}", '
                                  f'"ConfirmCode": "{self.confirm_code_value}"}}')
            self.login_headers = {**self.default_headers, "x-uuid": self.session_id}
            self.login_response = rq.post(url=self.login_url, headers=self.login_headers, data=self.login_payload)
            if self.login_response.status_code == 200:
                captcha_handler.update_captcha_value(
                    captcha_type="kaladade", captcha_id=self.captcha_id, captcha_value=self.captcha_value)
                self.access_token = self.login_response.json()["Data"]["access_token"]
                self.user_login_id = self.login_response.json()["Data"]["lgnd"]
                self._update_tokens(access_token=self.access_token, session_id=self.session_id, user_login_id=self.user_login_id)

    def _get_data(self, url, decode: bool = True) -> None:
        self._get_data_url = url
        self._get_data_headers = {
            **self.default_headers, "x-uuid": self.session_id, "authorization": "Bearer " + self.access_token}
        self._get_data_response = rq.get(url=self._get_data_url, headers=self._get_data_headers)
        self._get_data_response_json = self._get_data_response.json()
        if decode:
            decoder = KaladadeDecoder(kdc1=self.session_id, kdc2=self.user_login_id)
            self._data = decoder.decode(self._get_data_response_json)
        else:
            self._data = self._get_data_response_json

    def _post_data(self, url: str, data: dict, decode: bool = True):
        self._get_data_url = url
        self._get_data_headers = {
            **self.default_headers, "x-uuid": self.session_id, "authorization": "Bearer " + self.access_token}
        self._get_data_response = rq.post(url=self._get_data_url, headers=self._get_data_headers, data=json.dumps(data))
        self._get_data_response_json = self._get_data_response.json()
        if decode:
            decoder = KaladadeDecoder(kdc1=self.session_id, kdc2=self.user_login_id)
            self._data = decoder.decode(self._get_data_response_json)
        else:
            self._data = self._get_data_response_json

    def get_currencies(self) -> None:
        self._get_data(url=self.currencies_url)
        self.currencies_data = self._data["Data"]

    def get_categories(self):
        self._get_data(url=self.categories_url)
        self.categories_data = self._data["Data"]

    def get_market_watch(self, id1: int, id2: int):
        self._get_data(url=self.market_watch_url + f"?id1={id1}&id2={id2}")
        self.market_watch_data = self._data["Data"]

    def get_efc(self, id1: int, id2: int):
        self._get_data(url=self.efc_url + f"?id1={id1}&id2={id2}")
        self.efc_data = self._data["Data"]






if __name__ == "__main__":
    kaladade_agent = KaladadeAgent()
    kaladade_agent.login(phone_number="09021003706")
    kaladade_agent.get_market_watch(id1=10004, id2=20001)
    market_watch_data = kaladade_agent.market_watch_data
