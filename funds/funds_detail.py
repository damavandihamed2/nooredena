import warnings
import requests
import database
import pandas as pd
from tqdm import tqdm


warnings.filterwarnings("ignore")
powerbi_database = database.db_conn

comp_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en,fa;q=0.9',
    'Connection': 'keep-alive',
    # 'Referer': 'https://fund.fipiran.ir/mf/list?date=2024-10-22',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

comp_response = requests.get("https://fund.fipiran.ir/api/v1/fund/fundcompare",
                             headers=comp_headers)  # , params=comp_params)
comp_response_json = comp_response.json()
funds = pd.DataFrame(comp_response_json["items"])
funds_raw = funds.copy()
funds = funds[["regNo", "smallSymbolName"]]

get_headers = {
'Accept': 'application/json, text/plain, */*',
'Accept-Language': 'en,fa;q=0.9',
'Connection': 'keep-alive',
'Referer': f'https://fund.fipiran.ir/mf/profile/',
'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'same-origin',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
'sec-ch-ua-mobile': '?0',
'sec-ch-ua-platform': '"Windows"',
}

fundspage = pd.DataFrame()
fund_errors = []
for regno in tqdm(funds.regNo):
    try:
        get_params = {"regno": f"{regno}"}
        get_response = requests.get("https://fund.fipiran.ir/api/v1/fund/getfund",
                                    headers=get_headers, params=get_params, verify=False)
        get_response = get_response.json()["item"]
        df = pd.DataFrame([get_response])
        fundspage = pd.concat([fundspage, df], axis=0, ignore_index=True)
    except:
        fund_errors.append(regno)
    # time.sleep(0.5)


fundspage.drop(columns=['articlesOfAssociationLink', 'prosoectusLink', 'rankOf12Month', 'rankOf24Month',
                        'rankOf36Month', 'rankOf48Month', 'rankOf60Month', 'fundWatch', 'mutualFundLicenses'],
               inplace=True)
fundspage = fundspage.merge(funds, on="regNo", how="left")
fundspage["isCompleted"].replace({True: 1, False: 0}, inplace=True, regex=False)
fundspage["typeOfInvest"].replace({"IssuanceAndCancellation": 0, "Negotiable": 1}, inplace=True, regex=False)
fundspage.rename(mapper={"typeOfInvest": "ETF"}, axis=1, inplace=True)
fundspage["websiteAddress"] = ["" if fundspage["websiteAddress"].iloc[i] == []
                               else fundspage["websiteAddress"].iloc[i][0] for i in range(len(fundspage))]
fundspage = fundspage.astype({"fundSize": float, "dailyEfficiency": float, "weeklyEfficiency": float, "monthlyEfficiency": float,
                              "quarterlyEfficiency": float, "sixMonthEfficiency": float, "annualEfficiency": float,
                              "estimatedEarningRate": float, "guaranteedEarningRate": float,
                              "dividendIntervalPeriod": float, "insInvNo": float, "insInvPercent": float,
                              "legalPercent": float, "naturalPercent": float, "netAsset": float, "retInvNo": float,
                              "retInvPercent": float, "investedUnits": float, "unitsRedDAY": float,
                              "unitsRedFromFirst": float, "unitsSubDAY": float, "unitsSubFromFirst": float,
                              "efficiency": float, "cancelNav": float, "issueNav": float, "statisticalNav": float,
                              "fiveBest": float, 'stock': float, "bond": float, "other": float, "cash": float,
                              "deposit": float, "fundUnit": float, "commodity": float, "beta": float, "alpha": float,
                              'baseUnitsSubscriptionNAV': float, 'baseUnitsCancelNAV': float,
                              'baseUnitsTotalNetAssetValue': float, 'baseTotalUnit': float,
                              'baseUnitsTotalSubscription': float, 'baseUnitsTotalCancel': float,
                              'superUnitsSubscriptionNAV': float, 'superUnitsCancelNAV': float,
                              'superUnitsTotalNetAssetValue': float, 'superTotalUnit': float,
                              'superUnitsTotalSubscription': float, 'superUnitsTotalCancel': float})

crsr = powerbi_database.cursor()
crsr.execute("TRUNCATE TABLE [nooredenadb].[funds].[funds_detail_Data]")
crsr.commit()
crsr.close()
database.insert_to_database(dataframe=fundspage, database_table="[nooredenadb].[funds].[funds_detail_Data]")
