import io
import requests as rq
import PIL.Image as pil

from brokers.coinonline.utils.encrypt import encrypt_password
from brokers.coinonline.utils.extractor import extract_captcha_tag, extract_hash_code, extract_hdn_str_challenge, extract_trades, extract_trades_pagination


base_url = "https://coinonline.nibi.ir"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


response = rq.get(url=base_url, headers=headers)

session_id = response.cookies.get("ASP.NET_SessionId")  # this should update after fetching captcha image
TS01fd594c_token = response.cookies.get("TS01fd594c")   # this should update after each request

captcha_id = extract_captcha_tag(response_text=response.text)
hash_code = extract_hash_code(response_text=response.text)
hdnStrChallenge = extract_hdn_str_challenge(response_text=response.text)



captcha_url = f'{base_url}/{captcha_id}/JpegImage?PostFix='
captcha_headers = headers.copy()

cookies = {'TS01fd594c': TS01fd594c_token}
params = {'PostFix': ''}
response_captcha = rq.get(
    url=captcha_url.split("?")[0],
    headers=captcha_headers,
    cookies=cookies,
    params={'PostFix': ''}
)
session_id = response_captcha.cookies.get("ASP.NET_SessionId")
TS01fd594c_token = response_captcha.cookies.get("TS01fd594c")

img = response_captcha.content
img = pil.open(io.BytesIO(img))
img.resize(size=(img.size[0] * 3, img.size[1] * 3)).show()
captcha_value = input("Please Enter The Captcha Phrase: ")
img.close()




cookies = {'ASP.NET_SessionId': session_id, 'TS01fd594c': TS01fd594c_token}
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://coinonline.nibi.ir',
    'Pragma': 'no-cache',
    'Referer': 'https://coinonline.nibi.ir/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
data = {
    'txtUserName': '100200031390',
    'txtPassword': encrypt_password(password='Noore@dena10191', hash_code=hash_code)["pwd_enc"],
    'hdnStrChallenge': hdnStrChallenge,
    'txtCaptcha': captcha_value,
}

response_login = rq.post(
    'https://coinonline.nibi.ir/',
    cookies=cookies,
    headers=headers,
    data=data
)

skh_token = response_login.history[0].cookies.get(".SKH")
token = response_login.history[0].cookies.get("Token")
TS01fd594c_token = response_login.history[0].cookies.get("TS01fd594c")




cookies = {
    'ASP.NET_SessionId': session_id,
    '.SKH': skh_token,
    'Token': token,
    'TS01fd594c': TS01fd594c_token,
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://coinonline.nibi.ir',
    'Pragma': 'no-cache',
    'Referer': 'https://coinonline.nibi.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


markets_mapper = {
    "Future": '1',
    "Option": '2',
    "Deposit": '3'
}

##############################
##########  Future  ##########
##############################

data = {'OpenOnly': 'false', 'FromSama': 'false', 'market': markets_mapper["Future"]}
response = rq.post("https://coinonline.nibi.ir/Customer/GetImeFutureOrder", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': '1'}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineTodayTrades", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': '1'}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineCustomerState", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': '1'}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineCustomerTodayState", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': '1'}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineMatchedPositionsState", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})



##############################
##########  Deposit  #########
##############################


data = {'OpenOnly': 'true', 'FromSama': 'false', 'market': markets_mapper["Deposit"]}
response = rq.post("https://coinonline.nibi.ir/Customer/GetImeDepositOrder", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'OpenOnly': 'false', 'FromSama': 'false', 'market': markets_mapper["Deposit"]}
response = rq.post("https://coinonline.nibi.ir/Customer/GetImeDepositOrder", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': markets_mapper["Deposit"]}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineTodayTrades", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


response = rq.post('https://coinonline.nibi.ir/Customer/ImeOnlineCustomerDepositState', cookies=cookies, headers=headers)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})

response = rq.post('https://coinonline.nibi.ir/Customer/GetRealtimePortfolio', cookies=cookies, headers=headers)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})



##############################
##########  Option  ##########
##############################


data = {'OpenOnly': 'true', 'FromSama': 'false'}
response = rq.post('https://coinonline.nibi.ir/Customer/GetImeOptionOrder', cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'marketType': markets_mapper["Option"]}
response = rq.post('https://coinonline.nibi.ir/Customer/GetOnlineImeCustomerState', cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'OpenOnly': 'true', 'FromSama': 'false'}
response = rq.post("https://coinonline.nibi.ir/Customer/GetImeOptionOrder", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'OpenOnly': 'false', 'FromSama': 'false'}
response = rq.post("https://coinonline.nibi.ir/Customer/GetImeOptionOrder", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': markets_mapper["Option"]}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineTodayTrades", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


data = {'market': markets_mapper["Option"]}
response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineCustomerState", cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})


response = rq.post("https://coinonline.nibi.ir/Customer/ImeOnlineCustomerCoveredPositions", cookies=cookies, headers=headers)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})



##############################
##########  Trades  ##########
##############################

page_size = 20
page = 1

data = {'txtStartDate': '1405/02/20', 'txtEndDate': '1405/02/22',
        'OnlineImeTradesPagerHidden': f'{page}', 'OnlineImeTradespageIndex': f'{page_size}'}
response = rq.post('https://coinonline.nibi.ir/Customer/OnlineIMETrades', cookies=cookies, headers=headers, data=data)
cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})
# data = extract_trades(response.text)

pagination = extract_trades_pagination(response_text=response.text)
trades = extract_trades(response_text=response.text)

if pagination["page_numbers"] > 1:
    for page in range(2, pagination["page_numbers"] + 1):
        data = {'txtStartDate': '1405/02/20', 'txtEndDate': '1405/02/22',
                'OnlineImeTradesPagerHidden': f'{page}', 'OnlineImeTradespageIndex': f'{page_size}'}
        response = rq.post('https://coinonline.nibi.ir/Customer/OnlineIMETrades', cookies=cookies, headers=headers,
                           data=data)
        cookies.update({"TS01fd594c": response.cookies.get("TS01fd594c")})
        trades += extract_trades(response_text=response.text)

##############################
##########  UserInfo  ########
##############################

cookies_ =  {
    'ASP.NET_SessionId': '4xyvgjjf3lnlxrpvy1o333g4',
    '.SKH': '39274118CA93200C7438FB2A3CAAEFE727034C8AB621DCD71F5FA87D8911B29A156A4DF0CA5BE34DCD10D961128B6766E9EF1AC87D2574D601A7A5843D420832D618FE0821A85FA6CAA1F6FCC02F0B7E4EB2625F356837F51D275DC2C7CEB90C165C233126EADEF6A2A57350C51F0A548F11DF113F598355C09795A87145C7875C82F4A57719DDDB9E544697E4018B1E9DEE66BF935516AE2A15F7215B461CE6203E0CFCAD01D20C15E54664890BCC94',
    'Token': '0C269D11EE6B2278E5CA3D24E48B18DD841ACE1F4B59F5AB34BD2FFAF8E83EF75B20560B386DF4A400566F7DA9E50B7374CF3A32AE92BF3D81D91A262F3328E42A20B2B7F29C3B664BFE4288445BEA7187921621D7B81349B02C4D97C960769ECABB68094ACA3027ED30DBE8B47A7190972C058474C3C02064E24BB55BD3C1284FD7ACB721D5434C43F99AF60752A1171C1B6E372549DCED0D02A9D3D1D6D786A3AD3AF5F9E581AD68037B388568EEB2D5F4D74127F5498248521406167D7BF4D29AC66C1A2BB9333922AE65AB8DCFB2F5C576B0311B84692B229912D1B3A0C0',
    'TS01fd594c': '0180bb6f2270a52b2465879cf93ed1130e6c29230acb6ad09911d3a2042d82f8641feb3cc593366b253ec5e2822d7a69b45d35863eb89487d77cd56e8ca04c1ce6da805b962f1eaeb78b83540f0fcbbc0dafac57b42f1db9ccf2ec86be5dc7e57b527cd48baa71d5b4065399958a0b53bca47ec41379d770b440ae64d4d4199749c6f89fad'
}

cookies_ = {
    'TS01fd594c': '0180bb6f225ccf3fa9373e5fb27ec754726c003551e83ef0521e774d35f2876faa8aebf50a7c9b055576dd3ffc92b6b5d08620d891f26b977c194340b71ea515435c2ae4644bc5b94e0e7815048514e311998828e26ac52e3791f33c215171ddf2d1f65851d4949c865e205b3563985cbdcc95a8304d035f1ef9bec3b7356f8f68035854f9',
    'ASP.NET_SessionId': '03i0xdyst15xrjh32y2wx4du',
    '.SKH': '55351E61A4F3A6108501AEC1D9A8B1C3CD74A4C2379E0655EBB48A527C0CAA5A5CCE2E63CAD3A97D40A86BD603052533423DC6E89DFEC54B8E1027F4FA15568F6993DD56A1474362B4E0EEA61D2423574B96BE9CD3027019A598D345F4C4FA75ED687B683FBABCDDA7211C8E09E3482A43202FC4A1DAEDDB8876C7B5FE4C0FF3334984DF3625812845ADFEFA69293C7D06CD965C1CE4F86ADDC26B32BDC90E19463184F8C46F151D2D864E728D2B3AB4',
    'Token': '692213B63C5659544F98F21C6720376B9069571D13115C3EABCEF36D0041657D197EAFB9C9B14D215D48E479E7317A6BE85C46A686EC4EA8F5E071116816C554EC12C8E32FB6276AA8652CCB65A20B6E47E5E04427CD6A958E87F3797BB73DE29B17038BD01534AE432F1EEDB12300A41143E9D5E0361E49E4C6E3855F0D20C440AB2E8149598DC2C6938EF91547D087517D39080FB7D126F8E49C73CCD1610E09DD0274772AC3E1122D5FB76BA512A840A6CB39A66798DA774EF0E113A2AFD43559A3E7E0C603DDAEA7A11BC879F787D11E529C12C27F64266A119D9BCBE5E5'
}


response = rq.get('https://coinonline.nibi.ir/Customer/GetLastLoginInfo', cookies=cookies_, headers=headers)

try:
    response.json()
    print("Successful")
except rq.exceptions.JSONDecodeError:
    print("Invalid Cookies")


