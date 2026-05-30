import requests as rq
import uuid
from khobregan.omex import OmexAgent
from sigma.sigma_crawler import agent

auth = {"username": "2170262881", "password": "Dh@505684"}

agent = OmexAgent(address="https://khobregan.tsetab.ir/", username=auth["username"], password=auth["password"])
agent.login()
while True:
    agent.order_entry(instrument_id="IRO3DPDZ0001", side=1, price=2618, volume=30000)



json_data = {
    'PrincipalId': None,
    'InstrumentId': "IRO3DPDZ0001",
    'ISensOM': "Buy",
    'YValiOmNSC': 'Day',
    'DValiOM': None,
    'PLimSaiOM': 2618,
    'QTitTotOM': 30000,
    'QTitDvlOM': 0,
    'extraInfo': f'{{"ark":"{uuid.uuid4()}"}}',
    'ExecutionType': 'Instant',
}
order_entry_response = rq.post(
    url=agent.order_entry_url,
    headers=agent.headers,
    cookies=agent.cookies,
    json=json_data
)

order_entry_response.status_code
order_entry_response.json()

if order_entry_response.status_code == 200:

    if self.order_entry_response.json()["response"]["successful"]:
        self.order_entry_list.append(self.order_entry_response["response"]["data"])
else:

    try:
        response_json = order_entry_response.json()
        print(response_json)

    except Exception as e:
        print(e)
        print(f"Failed to place Order!")


{'response': {'successful': False,
  'count': None,
  'data': None,
  'errors': [{'message': 'تعداد باید در آستانه امروز باشد',
    'type': 'CustomException',
    'code': 'QuantityMustBeBetweenTheThreshold'}]}}

{'response': {'successful': False,
  'count': None,
  'data': None,
  'errors': [{'message': 'قیمت باید در آستانه امروز باشد',
    'type': 'CustomException',
    'code': 'PriceMustBeBetweenTheThreshold'}]}}

{'response': {'successful': False,
  'count': None,
  'data': None,
  'errors': [{'message': 'بیشینه مبلغ قابل استفاده 28,649,091 ریال است.  شما در تلاش برای استفاده 78,825,258 ريال هستید.',
    'type': 'CustomException',
    'code': 'WalletInsufficientBalance'}]}}

# Note: json_data will not be serialized by requests
# exactly as it was in the original request.
#data = '{"PrincipalId":null,"InstrumentId":"IRT3FATF0001","ISensOM":"Buy","YValiOmNSC":"Day","DValiOM":null,"PLimSaiOM":126237,"QTitTotOM":20,"QTitDvlOM":0,"extraInfo":"{\\"ark\\":\\"22a3314d-7995-4b13-a3f5-cf1d31b37305\\"}","ExecutionType":"Instant"}'
#response = requests.post('https://khobregan-red.tsetab.ir/api/Orders/OrderEntry', headers=headers, data=data)


import requests

cookies = {
    'cookiesession1': '678A8C66F98DF45710E6C04B31FCABB9',
    '_sk_kn_48451': '0nVEZhCZQlgSp3t',
}

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9,fa;q=0.8',
    'authorization': 'eyJhbGciOiJBMjU2R0NNS1ciLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwiaXYiOiJRYjByNmdVTm91TFBSbHNZIiwidGFnIjoiN1NaNVF5ZS1vX1NSZTR4QWh5M2J1ZyJ9.3NKhI7IYjOh0a9iWaJi3GHmqhoKS9xZhMq0jx4aC3U6MSIEhx4zE71UZvf_Va3k2kpOnpVoWDJvlZv5RriPHAw.PS_QDRVwH5jWt2N7rlz7mw.RQynJlFpn_LO4B5nXCXz1JNg4LBb3zQptQ5bvEnt8MiixQ2NIljsZyNnXoJYxagEqTdwhDd6Bd0XvDnKaSuVv9BoPLsV_xQ2itcWj6a1MsVDkWntKJk89lfRfm9GYmGJMxKeXa2cDmO0kKWiONM_hDi1cl3R0enEH88ALXpygCrmpT9_G0pACHyYZmk9OxO61Rpq81Z2vi3dYIhgHEUbBHBSfYP3Z8PzsIoKdK0m3IyQCIQiGrmnXCqb64eVE1Uk9-j2ob4iAduaHNMgRJTKFHTJA7cRpdOhBUt9axzScpYYK3pFMoV0FAfbxcVQrnu1hgIVJR7vu-7w8pYX1mTfS4EQCfo2PpNnpH1uMG3iAkHAOqMkSWDMK7d2nVBenHcPnd_3onMDyekX5U6rOFnjhMjlqn_E3QR2-5N-KsXBHxYwx3i_bXNekyRz-rioftMzbWN-ZB3EtlAglwtaHECVRbJZ3tqlmgU_TpXrCQNJ3DWmoInd788D7nKyauoxlfz8I7OyTCL8oUbRASE_9U7m_EdkJgX8PvmIGgKXBLZpCDoqlYxRtK94ohgmlPIhMIIiY89LrMjtFTh-iWmnf7SUokDCmoY0hzRqmXCF08rN5tFzsWCmL5m4OxRu5_65eHSNVyidtq3k1tIs04vhvWRlAxvGHoi2AUcNS4Wj2vU2OympwWzv31eZyaySpkHfY2Dj1TrSjkIihqHDtpE_Qfuyr5D5yCzlHeuweUkN4L6MoHAhmDn7d6QdBpYGZaG7K9ckOe4ruFkSQbaQgF8g3MIR3WpjE6vnLUdNbqbmFOZm6lTWqn0rM0zKgZMlEhika0U4CBIgDFR4vkScEa2CR7z6y7V5z0TIssYrvKx8NfpW8DuZjigPd1SmXHRoskIUEqfG9AKbmcVGTyH64KL7UdsVmg.1-b-jfhm0ZWk-2OdFq_6CS714QQUB9Ksq1JiUeLIhuI',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'ngsw-bypass': '',
    'origin': 'https://khobregan.tsetab.ir',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://khobregan.tsetab.ir/',
    'sec-ch-ua': '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
}

json_data = {
    'instrumentIds': [
        'IRO3DPDZ0001'
    ],
}
response = requests.post(
    'https://khobregan-red.tsetab.ir/api/PublicMessages/GetInstruments',
    cookies=cookies,
    headers=headers,
    json=json_data,
)
{'response': {'successful': True,
  'count': None,
  'data': [
      {'instrumentId': 'IRO3DPDZ0001',
    'lVal18AFC': 'دتوزیع',
    'lVal30': 'توزیع دارو پخش',
    'cGrValCot': 'Z1',
    'qPasCotFxeVal': 1.0,
    'qQtTranMarVal': 1,
    'qTitMinSaiOmProd': 1,
    'qTitMaxSaiOmProd': 800000,
    'baseVol': 1,
    'insCode': '66726992874614788',
    'zTitad': 30000000000,
    'ipo': False,
    'yVal': 303,
    'yDeComp': 2,
    'cSecVal': '43',
    'cSoSecVal': '4399',
    'cComVal': '3',
    'flow': 'Farabourse',
    'status': {'id': 2605206600087899,
     'instrumentId': 'IRO3DPDZ0001',
     'status': 'Authorized'},
    'marketType': 'Stock',
    'cIsin': 'IRO3DPDZ0002',
    'cSocCSAC': 'DPDZ',
    'underSupervision': 'Normal',
    'talInstrument': None,
    'yMarNSC': 'NO'}],
  'errors': None}}

