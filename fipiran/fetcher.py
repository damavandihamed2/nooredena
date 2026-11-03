import requests as rq

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.fipiran.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.fipiran.com/mf/list',
    'sec-ch-ua': '"Not:A-Brand";v="8", "Chromium";v="156", "Google Chrome";v="156"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36',
}




def get_fund_types() -> list[dict]:
    response = rq.get('https://fipiran.com/services/fund/fundtype', headers=headers)
    fund_types = response.json()["items"]
    return fund_types



def get_funds_all():

    json_data = {
        'regNos': [],
        'showMarketMakers': False,
    }

    response = rq.post('https://www.fipiran.com/services/fund/fundcompare', headers=headers, json=json_data)
    response.raise_for_status()

    response_json = response.json()
    all_funds = response_json.get("items")

    for f in all_funds:
        wa = f.get("websiteAddress")
        if len(wa) != 0:
            f.update({"websiteAddress": wa[0]})
        else:
            f.update({"websiteAddress": ""})

    return all_funds



def get_fund(reg_number: str, group_id: int) -> dict | None:

    params = {
        'regno': f'{reg_number}',
        'groupId': f'{group_id}',
    }
    response = rq.get(url=f"https://www.fipiran.com/services/fund/getfund", params=params, headers=headers)

    if response.status_code == 500 and  response.json()["error"]["httpErrorCode"] == 500:
        return None

    response_json = response.json()
    fund = response_json.get("item")
    wa = fund.get("websiteAddress")[0] if len(fund.get("websiteAddress")) != 0 else ""
    fund.update({"websiteAddress": wa})
    fund.pop("mutualFundLicenses")
    return fund

