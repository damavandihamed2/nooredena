tsetmc = "https://cdn.tsetmc.com/api"
old_setmc = "https://old.tsetmc.com"

##################################################
url_static_data = f"{tsetmc}/StaticData/GetStaticData"
payload_static_data = {}
headers_static_data = {
    "Origin": "https://www.tsetmc.com", "Accept-Language": "en-US,en;q=0.9", "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty", "Cache-Control": "no-cache", 'Connection': 'keep-alive', "Host": "cdn.tsetmc.com",
    "Sec-Fetch-Site": "same-site", "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd", "Pragma": "no-cache",
    "Sec-Ch-Ua-Mobile": "?0", "Sec-Fetch-Mode": "cors", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
}
##################################################
url_all_indices = f"{tsetmc}/Index/GetIndexB1LastAll/All/1"
payload_all_indices = {}
headers_all_indices = headers_static_data.copy()
##################################################
url_all_indices_farabourse = f"{tsetmc}/Index/GetIndexB1LastAll/All/2"
payload_all_indices_farabourse = {}
headers_all_indices_farabourse = headers_static_data.copy()
##################################################
url_sector_detail = f"{tsetmc}/MarketData/GetSectorTop/9999"
payload_sector_detail = {}
headers_sector_detail = headers_static_data.copy()
##################################################
url_main_page = f"{old_setmc}/Loader.aspx?ParTree=15"
payload_main_page = {}
headers_main_page = {
    "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-US,en;q=0.9", "Cache-Control": "no-cache",
    'Connection': 'keep-alive', "Host": "old.tsetmc.com", "Pragma": "no-cache", 'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
}
##################################################
url_market1_data = f"{tsetmc}/MarketData/GetMarketOverview/1"
payload_market1_data = {}
headers_market1_data = {
    "Sec-Ch-Ua-Mobile": "?0", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
    "Connection": "keep-alive", "Host": "cdn.tsetmc.com", "Sec-Ch-Ua-Platform": '"Windows"',
    "Accept-Language": "en-US,en;q=0.9", "Cache-Control": "no-cache", "Origin": "https://www.tsetmc.com",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', "Sec-Fetch-Site": "same-site",
    "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd", "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}
##################################################
url_market2_data = f"{tsetmc}/MarketData/GetMarketOverview/2"
payload_market2_data = {}
headers_market2_data = headers_market1_data.copy()
##################################################
url_marketwatch = f"{old_setmc}/tsev2/data/MarketWatchInit.aspx?h=0&r=0"
payload_marketwatch = {}
headers_marketwatch = {
    "Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "old.tsetmc.com", "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}
##################################################
url_clienttype = f"{old_setmc}/tsev2/data/ClientTypeAll.aspx"
payload_clienttype = {}
headers_clienttype = {
    "Accept": "text/plain, */*; q=0.01", "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "old.tsetmc.com", "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}
##################################################
url_search_name = f"{tsetmc}/Instrument/GetInstrumentSearch/"
payload_search_name = {}
headers_search_name = {
    "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate", "Host": "cdn.tsetmc.com",
    "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive", "Origin": "http://tsetmc.com",
    "Referer": "http://tsetmc.com/", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
}
##################################################
url_price_history = f"{tsetmc}/ClosingPrice/GetClosingPriceDailyList/"
payload_price_history = {}
headers_price_history = {
    'Cache-Control': 'no-cache', "Sec-Fetch-Site": "same-site", "Pragma": "no-cache",
    "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br, zstd",
    'Accept-Language': 'en-US,en;q=0.9', "Host": "cdn.tsetmc.com", "Origin": "https://www.tsetmc.com",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"', "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", 'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}
##################################################
url_price_last = "https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/"
payload_price_last = {}
headers_price_last = headers_price_history.copy()
##################################################
url_instrument_info = f"{tsetmc}/Instrument/GetInstrumentInfo/"
payload_instrument_info = {}
headers_instrument_info = {
    "Accept-Encoding": "gzip, deflate, br, zstd", "Accept": "application/json, text/plain, */*",
    "Upgrade-Insecure-Requests": "1", "Sec-Ch-Ua-Platform": '"Windows"', "Origin": "https://www.tsetmc.com",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', "Sec-Fetch-Mode": "cors",
    "Pragma": "no-cache", "Host": "cdn.tsetmc.com", "Sec-Ch-Ua-Mobile": "?0", "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "empty", 'Connection': 'keep-alive', "Cache-Control": "no-cache", "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
}
##################################################
url_instrument_identity = f"{tsetmc}/Instrument/GetInstrumentIdentity/"
payload_instrument_identity = {}
headers_instrument_identity = headers_instrument_info.copy()
##################################################
url_sector_companies = f"{tsetmc}/ClosingPrice/GetRelatedCompany/"
payload_sector_companies = {}
headers_sector_companies = headers_instrument_info.copy()
##################################################
url_sector_last_day = f"{tsetmc}/Index/GetIndexB1LastDay/"
payload_sector_last_day = {}
headers_sector_last_day = headers_instrument_info.copy()
##################################################
url_sector_history = f"{tsetmc}/Index/GetIndexB2History/"
payload_sector_history = {}
headers_sector_history = headers_instrument_info.copy()
##################################################
url_codal_reports_all = f"{tsetmc}/Codal/GetPreparedData/"
payload_codal_reports_all = {}
headers_codal_reports_all = {
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"', "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
    "Connection": "keep-alive", "Host": "cdn.tsetmc.com", "Pragma": "no-cache", "Sec-Ch-Ua-Platform": '"Windows"',
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.7", "Upgrade-Insecure-Requests": "1", "Cache-Control": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 "
                  "Safari/537.36", "Accept-Encoding": "gzip, deflate, br, zstd", "Accept-Language": "en-US,en;q=0.9"
}
##################################################
url_codal_reports_symbol = f"{tsetmc}/Codal/GetPreparedDataByInsCode/20/"
payload_codal_reports_symbol = {}
headers_codal_reports_symbol = headers_codal_reports_all.copy()
##################################################
url_state_all = f"{tsetmc}/MarketData/GetInstrumentStateTop/1"
payload_state_all = {}
header_state_all = headers_codal_reports_symbol.copy()
##################################################
url_state_symbol = f"{tsetmc}/MarketData/GetInstrumentStateAll/"
payload_state_symbol = {}
header_state_symbol = headers_codal_reports_symbol.copy()
##################################################
url_clienttype_symbol = f"{tsetmc}/ClientType/GetClientTypeHistory/"
payload_clienttype_symbol = {}
header_clienttype_symbol = headers_codal_reports_symbol.copy()
##################################################
url_symbol_price_info = f"{tsetmc}/ClosingPrice/GetClosingPriceInfo/"
payload_symbol_price_info = {}
headers_symbol_price_info = headers_codal_reports_symbol.copy()
##################################################
url_commodity_funds = f"{tsetmc}/ClosingPrice/GetTradeTop/CommodityFund/7/9999"
payload_commodity_funds = {}
headers_commodity_funds = headers_codal_reports_symbol.copy()
##################################################
url_option_market = f"{tsetmc}/Instrument/GetInstrumentOptionMarketWatch/0"
payload_option_market = {}
headers_option_market = {
    "Accept": "application/json, text/plain, */*", "Origin": "https://www.tsetmc.com", "Sec-Fetch-Dest": "empty",
    "Accept-Encoding": "gzip, deflate, br, zstd", "Accept-Language": "en-US,en;q=0.9", "Connection": "keep-alive",
    "Referer": "https://www.tsetmc.com/", "Host": "cdn.tsetmc.com", "Sec-Fetch-Mode": "cors",
    "sec-ch-ua-platform": '"Windows"', "Sec-Fetch-Site": "same-site", "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 "
                  "Safari/537.36", "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'}
##################################################
f"{tsetmc}/MarketData/GetInstrumentStatistic/3839324986781871"
