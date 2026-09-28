import io
import websocket
import requests as rq
import PIL.Image as pil

from brokers.exir.utils import generate_x_app_n


url_base = "https://sm.exirbroker.com"
headers_base = {
    'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Accept-Language': 'en-US,en;q=0.9', 'Pragma': 'no-cache',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 '
                  'Safari/537.36', 'sec-ch-ua': '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"',
    'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"'}
headers_login = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                           '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', 'Sec-Fetch-Dest': 'document',
                 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'none', 'Sec-Fetch-User': '?1',
                 'Upgrade-Insecure-Requests': '1'}
headers_other = {
    'Accept': 'application/json, text/plain, */*', 'Content-Type': 'application/json', 'Sec-Fetch-Site': 'same-origin',
    'Pragma': 'no-cache', 'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors'}


def get_login_page() -> str:
    response_login_page = rq.get(
        url=f'{url_base}/exir/login',
        headers={**headers_login, **headers_base}
    )
    cookiesession1 = response_login_page.cookies["cookiesession1"]
    return cookiesession1


def get_captcha(cookies_session: str) -> dict[str, str]:

    response_captcha = rq.get(
        url=f'{url_base}/captcha',
        headers={'Referer': f'{url_base}/exir/login', **headers_base},
        cookies={'cookiesession1': cookies_session}
    )
    client_login_id = response_captcha.cookies["client_login_id"]

    img = pil.open(io.BytesIO(response_captcha.content))
    img.resize(size=(img.size[0] * 3, img.size[1] * 3)).show()
    captcha_value = input("Please Enter The Captcha Phrase: ")
    img.close()
    return client_login_id, captcha_value


def login(
        username: str,
        password: str,
        cookies_session: str,
        client_login_id: str,
        captcha_value: str
):
    response_login = rq.post(
        url=f'{url_base}/api/v2/login',
        headers={'Origin': url_base, 'Referer': f'{url_base}/exir/login', 'client_login_id': client_login_id, **headers_base},
        cookies={'cookiesession1': cookies_session, "client_login_id": client_login_id},
        json={'brokerCode': 0, 'username': username, 'password': password, 'captcha': captcha_value, 'otp': ''}
    )
    response_login_json = response_login.json()
    return response_login_json

def get_assets(cookies_session: str, auth_token: str, nt: str):
    response_assets = rq.get(
        url=f'{url_base}/api/v5/user/asset',
        headers={'Referer': f'{url_base}/exir/mainNew', 'X-App-N': generate_x_app_n(url="/api/v5/user/asset", nt=nt), **headers_base},
        cookies={'cookiesession1': cookies_session, 'JWT-TOKEN': auth_token}
    )
    response_assets_json = response_assets.json()
    return response_assets_json



cookiesession1 = get_login_page()
client_login_id, captcha_value = get_captcha(cookies_session=cookiesession1)

login_json = login(
    username="", password="",
    cookies_session=cookiesession1, client_login_id=client_login_id, captcha_value=captcha_value
)

assets = get_assets(cookies_session=cookiesession1, auth_token=login_json["authToken"], nt=login_json["nt"])


rlcAuthHeader = login_json["rlcAuthHeader"]



def create_socket(rlcAuthHeader: str):
    ws = websocket.create_connection(
        url=f'wss://push1.irbroker.com/v2/ws?encoding=text&authToken={rlcAuthHeader}&device=web',
        header=["Origin: https://sm.exirbroker.com", "User-Agent: Mozilla/5.0",],
        timeout=10
    )

    try:
        print("Connected")

        first_message = ws.recv()
        print("Initial:", first_message)

        send_message = "&".join(
            [f"1,MW.{instrument_code}" for instrument_code in ["IRO1CHDN0001", "IRO1SEPA0001", "IRO3KHZZ0001"]]
        )
        ws.send(send_message)

        while True:
            try:
                print("Received:", ws.recv())
            except websocket.WebSocketTimeoutException:
                print("فعلاً پیام جدیدی نرسیده است.")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        ws.close()
