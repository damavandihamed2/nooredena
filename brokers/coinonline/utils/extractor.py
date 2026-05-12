import re
from bs4 import BeautifulSoup



header_name_mapper = {
    'ردیف': "row", 'شماره معامله': "trade_id", 'نماد': "symbol", 'طرف سفارش': "trade_type", 'قیمت': "price",
    'حجم': "volume", 'تاریخ': "date", 'زمان': "time", 'نرم افزار': "platform", 'وضعیت': "status"
}


def extract_hash_code(response_text: str) -> str | None:
    soup = BeautifulSoup(response_text, features="html.parser")
    script_tag = soup.find('script', string=re.compile("PasswordTextBoxId"))
    if script_tag:
        pattern = r"PasswordTextBoxId\s*=\s*'txtPassword';\s*var\s+hashCode\s*=\s*'([^']+)';"
        match = re.search(pattern, script_tag.string)
        if match:
            hash_code = match.group(1)
            return hash_code

def extract_captcha_tag(response_text: str) -> str | None:
    soup = BeautifulSoup(response_text, features="html.parser")
    captcha_img = soup.find("img", {"id": "Captcha"})
    match = re.search(r'/(\d+)/', captcha_img["src"])
    if match:
        result = match.group(1)
        return result


def extract_hdn_str_challenge(response_text: str) -> str | None:
    soup = BeautifulSoup(response_text, features="html.parser")
    hdn_str_challenge_tag = soup.find("input", {"id": "hdnStrChallenge"})
    hdn_str_challenge = hdn_str_challenge_tag.attrs["value"]
    return hdn_str_challenge


def extract_trades(response_text: str) -> list[dict]:

    soup = BeautifulSoup(response_text, 'html.parser')
    table = soup.find('table', {"id": "orderlist", "class": "GridTable"})

    table_header = table.find('thead').find_all("th")
    table_header = [h.text for h in table_header]
    table_header = [header_name_mapper[h] for h in table_header]

    table_rows = table.find('tbody').find_all("tr")
    table_rows = [[c.text for c in r.find_all("td")] for r in table_rows]

    data = [{
        table_header[cn]: r[cn].strip().split(" ")[0] if table_header[cn] == "symbol"
        else int(r[cn].strip().replace(",", "")) if r[cn].strip().replace(",", "").isdigit()
        else r[cn].strip().replace(",", "")
        for cn in range(len(table_header))
        } for r in table_rows]

    return data


def extract_trades_pagination(response_text: str) -> dict[str, int]:
    soup = BeautifulSoup(response_text, 'html.parser')

    page_box = soup.find('div', {"class": "page-box"})

    current_page = page_box.find("span")
    current_page = int(current_page.text)

    page_size = page_box.find('input', {"type": "text", "class": "pagesize", "id": "OnlineImeTradespageSize"})
    page_size = int(page_size.attrs["value"])

    page_numbers = page_box.find('input', {"type": "submit", "class": "pagesizetitle", "value": "صفحه آخر"})
    if page_numbers:
        match = re.search(r'(\d+)', page_numbers.attrs["onclick"])
        if match: page_numbers = int(match.group(1))
    else:
        page_list = page_box.find_all('input',
                                      {"type": "submit", "class": "pagesizetitle", "name": "OnlineImeTradesPager"})
        page_list = [page for page in page_list if page.attrs["value"] != 'صفحه بعد']
        match = re.search(r'(\d+)', page_list[-1].attrs["onclick"])
        if match: page_numbers = int(match.group(1))
    page_numbers = max(page_numbers, current_page)
    return {
        "page_numbers": page_numbers,
        "page_size": page_size,
        "current_page": current_page,
    }





