import requests
import pandas as pd
from kaladade import kaladade
from utils.database import make_connection, insert_to_database


db_connection = make_connection()
phone_number = "09372377126"
kaladade_agent = kaladade.KaladadeAgent()
kaladade_agent._check_old_tokens()
kaladade_agent.login(phone_number=phone_number)

kaladade_agent._get_data(url="https://api.kaladade.ir/api/report/config/Category?Id=30132&ReportType=511")
data = kaladade_agent._data

# categories = pd.read_sql("SELECT * FROM [nooredenadb].[kaladade].[categories]", db_connection)
# sub_categories = pd.read_sql("SELECT * FROM [nooredenadb].[kaladade].[sub_categories]", db_connection)
# assets = pd.read_sql("SELECT * FROM [nooredenadb].[kaladade].[assets]", db_connection)
# url_category = "https://api.kaladade.ir/api/report/config/Category?Id=300002&ReportType=511"
# kaladade_agent._get_data(url=url_category)
# data_category = kaladade_agent.data
# url_efc = "https://api.kaladade.ir/api/report/config/efc?id=300002"
# kaladade_agent._get_data(url=url_efc)
# data_efc = kaladade_agent.data


url_fnfuture = "https://api.kaladade.ir/api/Report/Category/FnFuture"

data_query = {
    'PageSize': 20,
    'TimeFrame': 3,
    'PriceType': 1,
    'SortField': 'GroupDate',
    'SortOrder': 'asc',
    'SortIndex': -1,
    'CellSortType': 1,
    'ValueMode': 1,
    'CurrencyType': 2,
    'DateNumber': 0,
    'CurrencyId2': 2,
    'CurrencyName': 'دلار',
    'ReportType': 511,
    'CategoryId': 300002,
    'ProducerId': 590001,
    'ReferenceId': 1011,
    'SubCatId': 22000695,
    'AlterNameId': 177000709,
    'CurrencyId1': 2,
    'UnitId': 7,
    'StartDateMiladi': 20251201,
    'EndDateMiladi': 20261231,
}

kaladade_agent._post_data(url=url_fnfuture, data=data_query)
data_fnfuture = kaladade_agent.data










headers_new = kaladade_agent.get_data_headers.copy()
# headers_new["content-type"] = 'application/json'
response = requests.post(url_fnfuture, data=data_query, headers=headers_new)




















# Note: json_data will not be serialized by requests
# exactly as it was in the original request.
#data = '{"PageSize":20,"TimeFrame":3,"PriceType":1,"SortField":"GroupDate","SortOrder":"asc","SortIndex":-1,"CellSortType":1,"ValueMode":1,"CurrencyType":2,"DateNumber":0,"CurrencyId2":2,"CurrencyName":"دلار","ReportType":511,"CategoryId":300002,"ProducerId":590001,"ReferenceId":1011,"SubCatId":22000695,"AlterNameId":177000709,"CurrencyId1":2,"UnitId":7,"StartDateMiladi":20251201,"EndDateMiladi":20261231}'.encode()
#response = requests.post('https://api.kaladade.ir/api/Report/Category/FnFuture', params=params, headers=headers, data=data)




"""
-------------
زغال سنگ کک شو داخلی چین
-------------
cid2
21151
-------------
cid3 (CategoryId)
300002
-------------
cid4 (SubCatId)
22001063
-------------
prdi (ProducerId)
590001
-------------
rfci (ReferenceId)
1008

-------------
alti (AlterNameId)
172000456

-------------
rt (ReportType)
511
-------------
un
7
-------------
cu1
2
-------------
cu2
2
-------------
cut
0
-------------
ila
1
-------------

"""

"""
TimeFrame: (1 : daily) (2 : weekly) (3 : monthly) (4 : ) (5 : ) (6 : )
CurrencyId1: (1 : rial) (2 : dollar)
CurrencyId2
CurrencyId2: (1006: dollar_azad) (10000: dollar_eskenas) (1002: dollar_nima)
"""

"""

https://api.kaladade.ir/api/Report/Category/FnFuture?
	ReportType=511&
	CategoryId=304378&
	ProducerId=510007&
	ReferenceId=1001&
	SubCatId=5003998&
	AlterNameId=155003998&
	TimeFrame=3& Monthly
	TimeFrame=2& weekly
	ValueMode=1&
	PriceType=1& # Mean Price
	PriceType=2& # Last Price
	CurrencyId1=2&
	CurrencyId2=2&
	UnitId=16&
	SortField=GroupDate&
	SortOrder=asc&
	CellSortType=1&
	PageSize=120&
	StartDateMiladi=20250501&
	EndDateMiladi=20260531

https://api.kaladade.ir/api/Report/Category/FnFuture?ReportType=511&CategoryId=304378&ProducerId=510007&ReferenceId=1001&
SubCatId=5003998&AlterNameId=155003998&
TimeFrame=3&
ValueMode=1&
PriceType=2&
CurrencyId1=2&CurrencyId2=2&UnitId=16&SortField=GroupDate&SortOrder=desc&CellSortType=1&PageSize=20&StartDateMiladi=20250501&EndDateMiladi=20260531
CurrencyId1=2&CurrencyId2=2&UnitId=16&SortField=GroupDate&SortOrder=desc&CellSortType=1&PageSize=8&StartDateMiladi=20250501&EndDateMiladi=20260531

StartDateMiladi=20260501&EndDateMiladi=20260531

https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=1002&PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I1002I2I16&From=1719360000&To=1748476800&CountBack=330
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=1002&PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I1002I2I16&From=0936390599&To=1748032199&CountBack=238
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=1&CurrencyId2=1002&PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I1I1002I2I16&From=1719360000&To=1748476800&CountBack=330
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=10000&PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I10000I2I16&From=1719360000&To=1748476800&CountBack=330
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=1006 &PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I1006 I2I16
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=10000&PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I10000I2I16
https://api.kaladade.ir/api/report/chart/history?TimeFrame=2&CurrencyId1=2&CurrencyId2=1002 &PriceType=2&ReportType=11&HasMultipleCurrency=false&Ticker=N1_511I304378I30000633I0_2I2I1002 I2I16
"""

