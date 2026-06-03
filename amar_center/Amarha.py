# شاخص قیمت

import requests
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path


BASE_STATS_URL = "https://amar.org.ir/statistical-information/statid"
SAVE_FOLDER = Path.cwd() / "amar_center" / "Stats Files"
SAVE_FOLDER.mkdir(parents=True, exist_ok=True)


# Send get request
def get_req(url: str) -> requests.models.Response:

    try:
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        return r
    except requests.exceptions.RequestException as e:
        print(f"Network error occurred: {e}")
        raise


# Get the HTML doc
def get_html(api_url: int) -> str:

    try:
        r = get_req(api_url)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print(f"Network error occurred: {e}")
    return html


# Find the download link, and publish date
def parse_html(file_html: str, api_url: str) -> dict:

    soup = BeautifulSoup(file_html, "html.parser")
    a = soup.select_one("a.downloadable[href]")
    if not a:
        raise RuntimeError("class downloadable not found")

    file_url = urljoin(api_url, a["href"])  # get the download url of the file
    data_id = a.get("data-id")
    container = a.find_parent("p")
    publish_date = None
    # find the publishing date, in the parent node of the download file
    if container:
        for sp in container.find_all("span"):
            text = sp.get_text(strip=True)
            if text.startswith("زمان انتشار:"):
                publish_date = text.replace("زمان انتشار:", "", 1).strip()
                break
    info = {
        "id": data_id,
        "url": file_url,
        "publish_date": publish_date,
        "link_text": a.get_text(strip=True),
    }
    return info


# Download the file
def download_file(file_url: str, save_filename: str) -> bytes:
    save_path = SAVE_FOLDER / save_filename

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)\
                      Chrome/132.0.0.0 Safari/537.36",
        "Referer": "https://amar.org.ir/"}

    try:
        # Send get request as stream
        with requests.get(file_url, headers=headers, stream=True, verify=False, timeout=60) as r:
            r.raise_for_status()

            # Open the destination file and write into it
            with open(save_path, "wb") as f:
                print("Downloading ...")
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # Divide the input into 1 Mb chunks
                    if chunk:
                        f.write(chunk)
            print(f"Successfully downloaded: {save_path}")
        return f
    except Exception as e:
        print(f"Error: {e}")


# Parse the date to get the year, month and day
def parse_date(date: str) -> tuple[int, int, int]:
    y, m, d = re.split(r"[-/]", date)
    return int(y), int(m), int(d)


# Return False if the last update date is before the given update date
def check_order(p_date: str, last_date: str):
    if last_date is None:
        return True
    else:
        s_y, s_m, s_d = parse_date(p_date)
        e_y, e_m, e_d = parse_date(last_date)
        if (e_y < s_y) or ((e_y == s_y) and (e_m < s_m)) or\
                ((e_y == s_y) and (e_m == s_m) and (e_d < s_d)):
            return True
        else:
            return False


def update_stats(conf_file: str = "config.json"):
    with open(conf_file, "r") as f_r:
        config_data = json.load(f_r)

    for item in config_data.items():
        stats_item = Stats(item[1])
        item = stats_item.get_file()
    with open(conf_file, "w", encoding="utf-8") as f_w:
        json.dump(config_data, f_w, ensure_ascii=False, indent=4)


class Stats:
    def __init__(self, stat_dict: dict):
        self.stat_dict = stat_dict
        self.stat_id = stat_dict.get('stat_id')
        self.save_name = stat_dict.get('save_name')
        self.publish_date = stat_dict.get('publish_date')
        self.file = None

    def get_file(self) -> dict:
        api_url = f"{BASE_STATS_URL}/{self.stat_id}"
        html = get_html(api_url)
        info = parse_html(html, api_url)
        if check_order(info.get('publish_date'), self.publish_date):
            self.file = download_file(info.get('url'), self.save_name + ".xlsx")
            self.update(info.get('publish_date'))
        return self.stat_dict

    def update(self, new_date: str) -> None:
        self.publish_date = new_date
        self.stat_dict['publish_date'] = new_date


########## Run Test #########

config_file = "amar_center/config.json"
update_stats(config_file)
