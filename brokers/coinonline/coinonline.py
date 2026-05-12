import jdatetime
import requests as rq

from utils import captcha_handler
from utils import auth_token_hadler

from brokers.coinonline.utils.encrypt import encrypt_password
from brokers.coinonline.utils.extractor import extract_captcha_tag, extract_hash_code, extract_hdn_str_challenge, extract_trades, extract_trades_pagination



class CoinOnline:
    def __init__(self, address: str, portfolio_id: int, username: str, password: str):
        self.portfolio_id = portfolio_id

        self.website_address = address.replace("https://", "").rstrip("/")
        self._username = username
        self._password = password

        self._user_is_login = False

        self.base_headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Origin': f"https://{self.website_address}",
        'Referer': f"https://{self.website_address}/",
        'Sec-Fetch-Site': 'same-origin',
        'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        }
        self.login_headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                              'Upgrade-Insecure-Requests': '1', 'Content-Type': 'application/x-www-form-urlencoded',
                              'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-User': '?1'}
        self.data_headers = {'Accept': '*/*', 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                             'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'X-Requested-With': 'XMLHttpRequest'}

        self.auth_cookies = {}
        self.response_captcha = self.captcha_id = self.captcha_tag = self.captcha_value = self.captcha_image = None
        self.hash_code = self.hdnStrChallenge = None

        self.response_login = None
        self.get_trades_each_page_response = None

    def _get_old_token(self) -> dict[str, str] | None:
        cookies = auth_token_hadler.get_tokens(app="coinonline", web_address=self.website_address,
                                               portfolio_id=self.portfolio_id)
        if cookies: return cookies

    def _update_token(self, cookies: dict) -> None:
        auth_token_hadler.update_tokens(app="coinonline", web_address=self.website_address,
                                        json_data=cookies, portfolio_id=self.portfolio_id)

    def _check_old_tokens(self) -> None:
        cookies_ = self._get_old_token()
        if cookies_:
            response = rq.get(f'https://{self.website_address}/Customer/GetLastLoginInfo',
                              cookies=cookies_, headers={**self.base_headers, **self.data_headers})
            try:
                response.json()
                print("old tokens are valid")
                self._user_is_login = True
                self.auth_cookies = cookies_
            except rq.exceptions.JSONDecodeError:
                print("old tokens has expired")
                pass


    def _get_login_page(self):
        response = rq.get(url=f"https://{self.website_address}", headers=self.base_headers)
        self.session_id = response.cookies.get("ASP.NET_SessionId")
        self.TS01fd594c_token = response.cookies.get("TS01fd594c")
        self.auth_cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})
        self.captcha_tag = extract_captcha_tag(response_text=response.text)
        self.hash_code = extract_hash_code(response_text=response.text)
        self.hdnStrChallenge = extract_hdn_str_challenge(response_text=response.text)

    def get_captcha(self):
        self.captcha_id = str(jdatetime.datetime.now().timestamp())
        self._get_login_page()
        captcha_url = f'https://{self.website_address}/{self.captcha_tag}/JpegImage'
        params = {'PostFix': ''}
        self.response_captcha = rq.get(url=captcha_url, headers=self.base_headers, cookies=self.auth_cookies, params=params)
        self.auth_cookies.update({"TS01fd594c": self.response_captcha.cookies.get("TS01fd594c"),
                                  "ASP.NET_SessionId": self.response_captcha.cookies.get("ASP.NET_SessionId")})
        self.captcha_image = self.response_captcha.content
        captcha_handler.save_captcha(
            captcha_type="coinonline",
            captcha_id=self.captcha_id,
            captcha_image=self.captcha_image,
            b64decode=False
        )

    def login(self):

        self._check_old_tokens()

        if self._user_is_login:
            pass
        else:
            self.get_captcha()
            self.captcha_value = captcha_handler.open_captcha(captcha_type="coinonline", captcha_id=self.captcha_id)

            self.response_login = rq.post(
                url=f"https://{self.website_address}", cookies=self.auth_cookies,
                headers={**self.base_headers, **self.login_headers},
                data={'txtUserName': self._username,
                      'txtPassword': encrypt_password(password=self._password, hash_code=self.hash_code)["pwd_enc"],
                      'hdnStrChallenge': self.hdnStrChallenge, 'txtCaptcha': self.captcha_value})
            self.auth_cookies.update({"TS01fd594c": self.response_login.history[0].cookies.get("TS01fd594c"),
                                      ".SKH": self.response_login.history[0].cookies.get(".SKH"),
                                      "Token": self.response_login.history[0].cookies.get("Token")})
            self._user_is_login = True
            self._update_token(self.auth_cookies)
            captcha_handler.update_captcha_value(captcha_type="coinonline", captcha_id=self.captcha_id,
                                                 captcha_value=self.captcha_value)


    def _fetch_data(self, url: str, data: dict | None = None) -> rq.Response:
        if data:
            response = rq.post(
                url=url, cookies=self.auth_cookies, headers={**self.base_headers, **self.data_headers}, data=data)
        else:
            response = rq.post(
                url=url, cookies=self.auth_cookies, headers={**self.base_headers, **self.data_headers})
        self.auth_cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})
        self._update_token(self.auth_cookies)
        return response


    def _get_trades_each_page(self, start_date: str, end_date: str, page: int, page_size: int) -> rq.Response:
        data = {'txtStartDate': start_date, 'txtEndDate': end_date,
                'OnlineImeTradesPagerHidden': f'{page}', 'OnlineImeTradespageIndex': f'{page_size}'}
        self.get_trades_each_page_response = self._fetch_data(
            url=f'https://{self.website_address}/Customer/OnlineIMETrades', data=data)
        return self.get_trades_each_page_response

    def get_trades(
            self, start_date: str, end_date: str, page_size: int = 20
    ):
        response = self._get_trades_each_page(
            start_date=start_date, end_date=end_date, page=1, page_size=page_size)
        trades = extract_trades(response.text)
        pagination = extract_trades_pagination(response.text)
        if pagination["page_numbers"] > 1:
            for page in range(2, pagination["page_numbers"] + 1):
                response = self._get_trades_each_page(start_date=start_date, end_date=end_date,
                                                      page=page, page_size=page_size)
                trades += extract_trades(response_text=response.text)
        return trades

