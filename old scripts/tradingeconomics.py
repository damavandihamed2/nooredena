import requests as rq
from bs4 import BeautifulSoup

head = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.7", "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,en-US;q=0.9", "Cache-Control": "no-cache", "Referer": "https://tradingeconomics.com/stocks",
    "Cookie": "_ga=GA1.1.972054365.1709748749; __gads=ID=238361869807adf6:T=1709750023:RT=1709750421:"
              "S=ALNI_MaxWAB5vst34_Z3xSBTxT3FR-HF7w; __gpi=UID=00000d6b271917c7:T=1709750023:RT=1709750421:"
              "S=ALNI_MY19xju51trly3xy9rOSm8NpIWxhQ; __eoi=ID=570c4756de99ef92:T=1709750023:RT=1709750421:"
              "S=AA-AfjbvgkWNWsQjKI5O_YGlAycD; ASP.NET_SessionId=qeesfh2hf1z3loa3zjn0jktg; TEServer=TEIIS; "
              "_ga_SZ14JCTXWQ=GS1.1.1709796516.2.1.1709796535.0.0.0", "Sec-Fetch-User": "?1",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"', "Sec-Fetch-Dest": "document",
    "Sec-Ch-Ua-Mobile": "?0", "Sec-Fetch-Mode": "navigate", "Sec-Ch-Ua-Platform": '"Windows"', "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 "
                  "Safari/537.36", "Sec-Fetch-Site": "same-origin", "Upgrade-Insecure-Requests": "1",}

cat = "commodities"
res = rq.get(f"https://tradingeconomics.com/{cat}", headers=head)

soup = BeautifulSoup(res.text, features="html.parser")
div = soup.find_all(name="div", class_="card")

for d in range(len(div)):

    d = 0

    thead = div[d].find_all(name="thead")
    th = thead[0].find_all(name="th")
    cols = [th[t].text.split()[0] for t in range(len(th))]
    subcat = cols[0]
    cols.remove(subcat)
    cols = ["Commodity", "Unit"] + cols

    tbody = div[d].find_all(name="tbody")
    tr = tbody[0].find_all(name="tr")

    for r in range(len(tr)):

        r = 0

        data = []
        td = tr[r].find_all(name="td")
        data.append(td[0].contents[1].contents[1].text)
        data.append(td[0].contents[3].text)

        for t in range(1, len(td)):
            data.append(td[t].text.split()[0])




div_2 = soup.find_all(name="div", class_="col-lg-7 col-md-7 col-sm-12 col-xs-12")
div_3 = soup.find_all(name="div", class_="col-lg-5 col-md-5 col-sm-12 col-xs-12")
atc = soup.find_all(name="div", class_="row graphBox")
