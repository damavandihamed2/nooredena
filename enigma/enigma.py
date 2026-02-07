import requests as rq
import json, ssl, datetime, websocket
from typing import Optional, Callable, Literal

from utils import captcha_handler
from utils import auth_token_hadler
from utils.database import make_connection



class EnigmaAgent:

    report_types = ["balance_sheet", "income_statement", "monthly_report", "codal_watch", "products_and_sales",
                    "cost", "inventory_turnover", "raw_materials", "production_overhead"]
    default_headers = {
        "Accept-Encoding": "gzip, deflate, br", "Content-Type": "application/json", "Origin": "https://panel.enigma.ir",
        "Accept-Language": "fa", "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"Windows"', "Sec-Fetch-Dest": "empty",
        "Connection": "keep-alive", "Host": "core.enigma.ir", "Accept": "application/json", "Sec-Fetch-Mode": "cors",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115"
                      ".0.0.0 Safari/537.36", "Referer": "https://panel.enigma.ir/", "Sec-Fetch-Site": "same-site",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"'}
    check_token_url = "https://core.live.enigma.ir/api/v1/options/user_preferences/"
    captcha_url = "https://core.enigma.ir/api/v1/captcha/"
    captcha_headers = {**default_headers, "Content-Length": "2"}
    captcha_payload = "{}"
    login_url = "https://core.enigma.ir/api/v1/account/login/"
    login_headers = {**default_headers}
    refresh_token_url = "https://core.enigma.ir/api/v1/account/refresh/"
    commodity_url = "https://core.enigma.ir/api/v1/commodities/comparing/"
    commodity_all_url = "https://core.enigma.ir/api/v1/commodities/market_data/"
    codal_watch_url = "https://core.enigma.ir/api/v1/data_export/codal_watch/"
    market_indices_url = "https://core.live.enigma.ir/api/v1/dashboard/market_indices/"
    search_url = f"https://core.enigma.ir/api/v1/securities/search/?security_type=1&search="
    watchlist_url = "https://core.live.enigma.ir/api/v1/market_watch/watchlist/all/"
    ws_url = "wss://core.live.enigma.ir/ws/v1"

    def __init__(self, username, password):

        self.username = username
        self.password = password
        self.check_token_headers = self.check_token_response = None
        self.is_old_token_valid = False

        self.captcha_id = self.captcha_key = self.captcha_image = self.captcha_value = None
        self.login_payload = self.login_response = self.access_token = self.refresh_token = None
        self.refresh_token_headers = self.refresh_token_payload = self.refresh_token_response = None
        self.commodity_headers = self.commodity_payload = self.commodity_response = self.commodity_data = None
        self.commodity_all_response = self.commodity_all_headers = self.commodity_all_data = None
        self.codal_watch_headers = self.codal_watch_response = self.codal_watch_data = None
        self.market_indices_headers = self.market_indices_response = self.market_indices_data = None
        self.search_headers = self.search_response = self.search_data = None
        self.report_url = self.report_headers = self.report_response = self.report_data = None
        self.watchlist_headers = self.watchlist_response = self.watchlist_data = None
        self.symbol_url = self.symbol_headers = self.symbol_ws_conn = None
        self.symbol_data = self.symbol_messages = None
        self.dashboard_url = self.dashboard_headers = self.dashboard_ws_conn = None
        self.dashboard_data = self.dashboard_messages = None

    def _handle_response(self, response: rq.Response,
                         refresh_func: Optional[Callable[[], None]],
                         name_err: str = None) -> Optional[bool]:
        if response.status_code == 401:
            print("You token has expired, refreshing token...")
            refresh_func()
            return False
        elif response.status_code == 200:
            pass
        elif response.status_code == 402:
            print("Your subscription has ended.")
        elif response.status_code == 400:
            print("Requested page doesn't exist." + f" - {name_err}")
        elif response.status_code == 404:
            print("Requested page doesn't exist." + f" - {name_err}")
        else:
            print("Unknown error has happened.", response.status_code, " - ", response.text)
        return True

    def _get_old_token(self) -> [str, str]:
        tokens = auth_token_hadler.get_tokens(app="enigma", web_address="enigma")
        if tokens:
            try:
                access_token, refresh_token = tokens["access_token"], tokens["refresh_token"]
                return access_token, refresh_token
            except Exception:
                pass
        return "", ""

    def _update_tokens(self, access_token: str, refresh_token: str) -> None:
        json_data = {"access_token": access_token, "refresh_token": refresh_token}
        auth_token_hadler.update_tokens(app="enigma", web_address="enigma", json_data=json_data)

    def _refresh_access_token(self) -> None:
        self.refresh_token_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        self.refresh_token_payload = json.dumps({"refresh": f"{self.refresh_token}"})
        self.refresh_token_response = rq.post(
            url=EnigmaAgent.refresh_token_url, headers=self.refresh_token_headers, data=self.refresh_token_payload)
        if self.refresh_token_response.status_code == 200:
            self.access_token = json.loads(self.refresh_token_response.text)["data"]["access"]
            self._update_tokens(access_token=self.access_token, refresh_token=self.refresh_token)

    def check_old_tokens(self):
        self.access_token, self.refresh_token = self._get_old_token()
        self.check_token_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        self.check_token_response = rq.get(url=EnigmaAgent.check_token_url, headers=self.check_token_headers)
        if self.check_token_response.status_code == 200:
            self.is_old_token_valid = True
        elif self.check_token_response.status_code == 401:
            self._refresh_access_token()
            if self.refresh_token_response.status_code == 200:
                self.is_old_token_valid = True
            else:
                self.is_old_token_valid = False
        else:
            self.is_old_token_valid = False

    def get_captcha(self) -> None:
        self.captcha_id = str(datetime.datetime.now().timestamp())
        captcha_response = rq.post(url=EnigmaAgent.captcha_url, headers=EnigmaAgent.captcha_headers, data=EnigmaAgent.captcha_payload)
        captcha_data = captcha_response.json()["data"]
        self.captcha_image, self.captcha_key = captcha_data["captcha_image"], captcha_data["captcha_key"]
        captcha_handler.save_captcha(captcha_type="enigma", captcha_image=self.captcha_image,
                                     captcha_id=self.captcha_id)

    def login(self) -> None:
        self.check_old_tokens()
        if self.is_old_token_valid:
            pass
        else:
            self.get_captcha()
            self.captcha_value = captcha_handler.open_captcha(captcha_type="enigma", captcha_id=self.captcha_id)
            self.login_payload = json.dumps({"emailOrPhone": self.username, "phone_number": self.username,
                                            "password": self.password, "captcha_value": self.captcha_value,
                                            "captcha_key": self.captcha_key})
            login_response = rq.post(url=EnigmaAgent.login_url, headers=EnigmaAgent.login_headers, data=self.login_payload)
            self.login_response = login_response
            if self.login_response.status_code == 200:
                self.access_token = json.loads(self.login_response.text)["data"]["token"]["access"]
                self.refresh_token = json.loads(self.login_response.text)["data"]["token"]["refresh"]
                captcha_handler.update_captcha_value(captcha_type="enigma", captcha_id=self.captcha_id,
                                                     captcha_value=self.captcha_value)
                self._update_tokens(access_token=self.access_token, refresh_token=self.refresh_token)

    def __commodities_request(self, commodities_list: list[str]) -> rq.Response:
        self.commodity_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        self.commodity_payload = json.dumps(
            [{"first_operand_slug": commodities_list[i], "index": i + 1} for i in range(len(commodities_list))])
        response = rq.post(url=EnigmaAgent.commodity_url, headers=self.commodity_headers, data=self.commodity_payload)
        return response

    def get_commodities(self, commodities_list: list[str]) -> None:
        while True:
            self.commodity_response = self.__commodities_request(commodities_list=commodities_list)
            result = self._handle_response(
                response=self.commodity_response, refresh_func=self._refresh_access_token, name_err=commodities_list[0])
            if result is True: break
            else: pass
        if "data" in self.commodity_response.json().keys():
            self.commodity_data = self.commodity_response.json()["data"]

    def __commodity_all_request(self) -> rq.Response:
        self.commodity_all_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=EnigmaAgent.commodity_all_url, headers=self.commodity_all_headers)
        return response

    def get_commodity_all(self):
        while True:
            self.commodity_all_response = self.__commodity_all_request()
            result = self._handle_response(
                response=self.commodity_all_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        if "data" in self.commodity_all_response.json().keys():
            self.commodity_all_data = self.commodity_all_response.json()["data"]

    def __codal_watch_request(self) -> rq.Response:
        self.codal_watch_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=EnigmaAgent.codal_watch_url, headers=self.codal_watch_headers)
        return response

    def get_codal_watch(self):
        while True:
            self.codal_watch_response = self.__codal_watch_request()
            result = self._handle_response(response=self.codal_watch_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        if "data" in self.codal_watch_response.json().keys():
            self.codal_watch_data = self.codal_watch_response.json()["data"]

    def __market_indices_request(self) -> rq.Response:
        self.market_indices_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=EnigmaAgent.market_indices_url, headers=self.market_indices_headers)
        return response

    def get_market_indices(self):
        while True:
            self.market_indices_response = self.__market_indices_request()
            result = self._handle_response(
                response=self.market_indices_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        self.market_indices_data = self.market_indices_response.json()

    def __search_request(self, symbol: str) -> rq.Response:
        self.search_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=EnigmaAgent.search_url + f"{symbol}", headers=self.search_headers)
        return response

    def search(self, symbol: str):
        while True:
            self.search_response = self.__search_request(symbol=symbol)
            result = self._handle_response(response=self.search_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        if "data" in self.search_response.json().keys():
            self.search_data = self.search_response.json()["data"]

    def __report_request(self, report_type: Literal[*report_types], symbol_id: str,
                         start_date: str, end_date: str) -> rq.Response:
        report_payload = f"?start_date={start_date}&end_date={end_date}&daily=false"
        rprt = "https://core.enigma.ir/api/v{api_version}/data_export"
        if report_type not in EnigmaAgent.report_types:
            raise ValueError(f"{report_type} is not a valid report type, accepted types are {EnigmaAgent.report_types}")
        elif report_type == "balance_sheet":
            api_version = 2
            self.report_url = f"{rprt.format(api_version=api_version)}/{report_type}/{symbol_id}/{report_payload}"
        else:
            api_version = 1
            if report_type == "financial_ratios":
                self.report_url = (f"{rprt.format(api_version=api_version)}/codal_watch/"
                                   f"{symbol_id}/{report_type}/{report_payload}")
            else:
                self.report_url = f"{rprt.format(api_version=api_version)}/{report_type}/{symbol_id}/{report_payload}"
        self.report_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=self.report_url, headers=self.report_headers)

        return response

    def get_report(self, report_type: Literal[*report_types], symbol_id: str, start_date: str, end_date: str) -> None:
        while True:
            self.report_response = self.__report_request(report_type=report_type, symbol_id=symbol_id,
                                                         start_date=start_date, end_date=end_date)
            result = self._handle_response(response=self.report_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        if "data" in self.report_response.json().keys():
            self.report_data = self.report_response.json()["data"]

    def __watchlist_request(self) -> rq.Response:
        self.watchlist_headers = {**EnigmaAgent.default_headers, "Authorization": f"Bearer {self.access_token}"}
        response = rq.get(url=EnigmaAgent.watchlist_url, headers=self.watchlist_headers)
        return response

    def get_watchlist(self) -> None:
        while True:
            self.watchlist_response = self.__watchlist_request()
            result = self._handle_response(response=self.watchlist_response, refresh_func=self._refresh_access_token)
            if result is True: break
            else: pass
        self.watchlist_data = self.watchlist_response.json()

    def _symbol_ws(self, symbol_code: str) -> websocket.WebSocket:

        self.symbol_url = f"{EnigmaAgent.ws_url}/security/{symbol_code}/"
        self.symbol_headers = {**EnigmaAgent.default_headers,
                               "Sec-WebSocket-Protocol": f"{self.access_token}, panel"}
        ws = websocket.create_connection(
            url=self.symbol_url, header=self.symbol_headers, sslopt={"cert_reqs": ssl.CERT_NONE})
        return ws

    def get_symbol(self, symbol_code: str):
        self.symbol_ws_conn = self._symbol_ws(symbol_code)
        self.symbol_messages = []
        for i in range(12):
            result = self.symbol_ws_conn.recv()
            self.symbol_messages.append(json.loads(result))
        self.symbol_ws_conn.close()
        symbol_data = {}
        for m in range(len(self.symbol_messages)):
            main_key, data = list(self.symbol_messages[m].keys())[0], self.symbol_messages[m][
                list(self.symbol_messages[m].keys())[0]]
            if main_key == "best_list":
                if main_key not in symbol_data: symbol_data[main_key] = []
                symbol_data[main_key].append(data)
            else:
                symbol_data[main_key] = data
        self.symbol_data = symbol_data

    def _dashboard_ws(self) -> websocket.WebSocket:
        self.dashboard_url = f"{EnigmaAgent.ws_url}/dashboard/"
        self.dashboard_headers = {**EnigmaAgent.default_headers,
                               "Sec-WebSocket-Protocol": f"{self.access_token}, panel"}
        ws = websocket.create_connection(
            url=self.dashboard_url, header=self.dashboard_headers, sslopt={"cert_reqs": ssl.CERT_NONE})
        return ws

    def get_dashboard(self):
        self.dashboard_ws_conn = self._dashboard_ws()
        self.dashboard_messages = []
        while True:
            dashboard_data = self.dashboard_ws_conn.recv()
            self.dashboard_messages.append(dashboard_data)
            dashboard_data = json.loads(dashboard_data)
            if "dashboard_market_indices" in dashboard_data.keys(): break
        self.dashboard_ws_conn.close()
        self.dashboard_data = dashboard_data

#######################################################################################################################

if __name__ == "__main__":
    authenticator = [{"username": "09210882478", "password": "MMR123456"}]
    enigma_agent = EnigmaAgent(username=authenticator[0]["username"], password=authenticator[0]["password"])
    enigma_agent.check_old_tokens()
    enigma_agent.login()
    enigma_agent.get_watchlist()
    watchlist_data = enigma_agent.watchlist_data

    # symbol = "دتولید"
    symbol_id = "0d2f8e6f-4ca3-43d0-86d7-a1fc36850e6b"
    report_type, start_date, end_date = "monthly_report", "2024-12-21", "2025-09-22"
    enigma_agent.get_report(report_type=report_type, symbol_id=symbol_id, start_date=start_date, end_date=end_date)
    data = enigma_agent.report_data


