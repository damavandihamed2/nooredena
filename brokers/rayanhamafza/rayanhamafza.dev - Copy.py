import requests as rq
import warnings, jdatetime
from typing import Literal

from utils import captcha_handler


warnings.filterwarnings("ignore")



class BrokersRayanhamafza:

    def __init__(self, url: str, username: str, password: str):

        self.headers_ = {'Accept': 'text/html, */*; q=0.01', 'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'no-cache',
                    'sec-ch-ua-platform': '"Windows"', 'Connection': 'keep-alive', 'Pragma': 'no-cache',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/129.0.0.0 Safari/537.36'}



        self.url = url
        self.username = username
        self.password = password

        self.login_page_url = self.url + "Account/Login"
        self.captcha_url = self.url + "/api/brokerage/Captcha"
        self.captcha_headers = None
        self.account_url = self.url + "api/customer/Account"
        self.remaining_asset_url = self.url + "api/Customer/RemainingAsset"

        self.rayanAntiforgeryField = None
        self.brokerageCustomerPanelAntiforgeryCookie = None

        self.captcha_content = None
        self.brokerageCustomerPanelSession = None
        self.captcha_id = None
        self.captcha_tracking_number = None

        self.captcha_value = None
        self.brokerageCustomerPanelSessionCustomer = None
        self.loginResponseStatus = None

        self.purchase_upper_bound_url = self.url + "Api/Customer/Info/PurchaseUpperBoundStocks"
        self.assets = None
        self.assets_value = None
        self.purchase_upper_bound = None

        self.trades_url = self.url + "api/customer/Statements?WithDetail=true&WithPreBalance=true"
        self.trades_start_date = None
        self.trades_end_date = None
        self.trades = None
        self.customer_last_remain = None


    def create_cookies(self):
        cookies = {'Cookie': f'BrokerageCustomerPanel_AntiforgeryCookie={self.brokerageCustomerPanelAntiforgeryCookie};'
                             f'BrokerageCustomerPanel.Session={self.brokerageCustomerPanelSession};'
                             f'BrokerageCustomerPanel.Session.Customer={self.brokerageCustomerPanelSessionCustomer};'}

    def login_page(self):
        login_response = rq.get(url=self.login_page_url)
        self.rayanAntiforgeryField = login_response.text.split(
            'name="Rayan_AntiforgeryField" type="hidden" value="')[1].split('" /></form>')[0]
        self.brokerageCustomerPanelAntiforgeryCookie = login_response.headers["set-cookie"].split(
            'BrokerageCustomerPanel_AntiforgeryCookie=')[1].split(';')[0]

    def captcha(self):

        self.login_page()

        self.captcha_id = str(jdatetime.datetime.now().timestamp())
        self.captcha_headers = {
            "cookie": f"BrokerageCustomerPanel_AntiforgeryCookie={self.brokerageCustomerPanelAntiforgeryCookie}"
        }
        captcha_response = rq.get(url=self.captcha_url, headers=self.captcha_headers)
        self.captcha_content = captcha_response.json()["data"]
        self.captcha_tracking_number = captcha_response.json()["trackingNumber"]
        captcha_handler.save_captcha(captcha_type="rayanhamafza", captcha_image=self.captcha_content,
                                     captcha_id=self.captcha_id, b64decode=True)


    def login(self):

        self.captcha()

        self.captcha_value = captcha_handler.open_captcha(captcha_type="rayanhamafza", captcha_id=self.captcha_id)

        account_payload = (f"Username={self.username}&Password={self.password}&Captcha={self.captcha_value}&"
                           f"CaptchaTrackingNumber={self.captcha_tracking_number}&LoginType=StaticPassword&"
                           f"Rayan_AntiforgeryField={self.rayanAntiforgeryField}")
        account_header = {
            **self.headers_,
            'Cookie': f'BrokerageCustomerPanel_AntiforgeryCookie={self.brokerageCustomerPanelAntiforgeryCookie};'
        }
        account_response = rq.post(url=self.account_url, headers=account_header, data=account_payload)
        if (account_response.status_code == 200) and (account_response.json()["success"]):
            try:
                captcha_handler.update_captcha_value(captcha_type="rayanhamafza", captcha_id=self.captcha_id,
                                                     captcha_value=self.captcha_value)
            except Exception as e:
                print(e)
            self.brokerageCustomerPanelSessionCustomer = account_response.headers["Set-Cookie"].split(
                "BrokerageCustomerPanel.Session.Customer=")[1].split(";")[0]
            self.brokerageCustomerPanelSession = account_response.headers["Set-Cookie"].split(
                "BrokerageCustomerPanel.Session=")[1].split(";")[0]

            self.loginResponseStatus = 200
        else:
            self.loginResponseStatus = account_response.status_code

    def get_purchase_upper_bound(self):
        if self.loginResponseStatus != 200:
            self.login()
        else:
            pass
        purchase_upper_bound_headers = {
            **self.headers_,
            'Cookie': f'BrokerageCustomerPanel_AntiforgeryCookie={self.brokerageCustomerPanelAntiforgeryCookie};'
                      f'BrokerageCustomerPanel.Session={self.brokerageCustomerPanelSession};'
                      f'BrokerageCustomerPanel.Session.Customer={self.brokerageCustomerPanelSessionCustomer};'}
        purchase_upper_bound_response = rq.get(url=self.purchase_upper_bound_url, headers=purchase_upper_bound_headers)
        if purchase_upper_bound_response.status_code == 200:
            try:
                self.purchase_upper_bound = purchase_upper_bound_response.json()
            except Exception as e:
                print(e)
                return "Getting purchaseUpperBound failed."
        else:
            return purchase_upper_bound_response.status_code




import io
import base64
import easyocr
import requests as rq
import PIL.Image as pil
import warnings, jdatetime
from utils import captcha_handler

warnings.filterwarnings("ignore")

url = "https://parsian.irbrokersite.ir/"
username = "44280113440"
password = "npdjA5Jmq9"

login_response = rq.get(url=url + "Account/Login")
rayanAntiforgeryField = login_response.text.split('name="Rayan_AntiforgeryField" type="hidden" value="')[1].split('" /></form>')[0]
brokerageCustomerPanelAntiforgeryCookie = login_response.headers["set-cookie"].split('BrokerageCustomerPanel_AntiforgeryCookie=')[1].split(';')[0]

captcha_headers = {"cookie": f"BrokerageCustomerPanel_AntiforgeryCookie={brokerageCustomerPanelAntiforgeryCookie}"}
captcha_response = rq.get(url=url + "/api/brokerage/Captcha", headers=captcha_headers)
captcha_content = captcha_response.json()["data"]
captcha_tracking_number = captcha_response.json()["trackingNumber"]
image = base64.b64decode(captcha_content)

img = pil.open(io.BytesIO(image))
img.save("./brokers/tmp/captcha.jpg")
img.resize(size=(img.size[0] * 3, img.size[1] * 3)).show()
img.close()

reader = easyocr.Reader(["en"], gpu=False)
result = reader.readtext(image=image, detail=1, allowlist="+0123456789")
result_2 = reader.readtext(image="./brokers/tmp/captcha.jpg", detail=1, allowlist="+0123456789")






image_path = "D:/PythonProjects/portfolios/images/4.png"
reader = easyocr.Reader(["en"], gpu=False)
result = reader.readtext(image_path, detail=0,
                         # allowlist="+0123456789"
                         )
print(result)