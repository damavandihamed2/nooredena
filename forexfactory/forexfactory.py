import cloudscraper

headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
                     'application/signed-exchange;v=b3;q=0.7', 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate',
           'Accept-Encoding': 'gzip, deflate, br, zstd', 'Sec-Ch-Ua-Platform': '"Windows"', 'Cache-Control': 'no-cache',
           'Accept-Language': 'en-US,en;q=0.9', 'Pragma': 'no-cache', 'Priority': 'u=0, i', 'Sec-Fetch-Site': 'none',
           'Sec-Ch-Ua': '"Chromium";v="136", "Google Chrome";v="136", "Not)A;Brand";v="99"', 'Sec-Ch-Ua-Mobile': '?0',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/136.0.0.0 Safari/537.36', 'Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1'}
scraper = cloudscraper.create_scraper()
url = 'https://www.forexfactory.com/calendar?day=may26.2025'
response = scraper.get(url, headers=headers)
