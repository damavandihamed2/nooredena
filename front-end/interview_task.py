import json
import pandas as pd
from pathlib import Path

from utils.database import make_connection

db_conn = make_connection()

commodities_df = pd.read_sql("SELECT date,price,commodity,unit,reference FROM [nooredenadb].[commodity].[commodities_data]"
                             " WHERE commodity In ('اسپرد کنسانتره و شمش (ریال)','شیر خام','کرک شگویا','اسپرد ورق و اسلب','نفتا','گاز مایع','اتیلن پلتس','درهم امارات','اوره')", db_conn)

commodities_df["date"] = commodities_df["date"] + "T00:00:00+03:30"

commodities_unique_df = commodities_df[["commodity", "unit", "reference"]]
commodities_unique_df.drop_duplicates(inplace=True, ignore_index=True, keep='first')

json_dict = dict()

json_dict["commodities"] = commodities_unique_df.to_dict(orient='records')

data = dict()
for c in range(len(commodities_unique_df)):
    commodity = commodities_unique_df["commodity"].iloc[c]
    unit = commodities_unique_df["unit"].iloc[c]
    reference = commodities_unique_df["reference"].iloc[c]

    tmp = commodities_df[(commodities_df["commodity"] == commodity) & (commodities_df["unit"] == unit) & (commodities_df["reference"] == reference)].reset_index(drop=True)
    tmp["open"] = tmp["price"]
    tmp["high"] = tmp["price"]
    tmp["low"] = tmp["price"]
    tmp["close"] = tmp["price"]
    tmp = tmp[["date", "open", "high", "low", "close"]]
    tmp = tmp.to_dict(orient="records")
    data[commodity] = tmp

json_dict["data"] = data

json_str = json.dumps(json_dict, ensure_ascii=False, separators=(",", ":"))

Path("./front-end/commodities_data.json").write_text(json_str, encoding="utf-8")
