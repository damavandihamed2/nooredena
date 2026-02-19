import numpy as np
import pandas as pd
from tqdm import tqdm
import warnings, jdatetime
from functools import reduce

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
result_path = "D:/database/commodity/results.xlsx"
today = jdatetime.datetime.today()
db_conn = make_connection()

dim_date = pd.read_sql("SELECT TRY_CONVERT(VARCHAR, Miladi) as Miladi, REPLACE(Jalali_1, '/', '-') as date "
                       "FROM [nooredenadb].[extra].[dim_date]", db_conn)

query_oil = ("SELECT COALESCE(b.date, d.date, o.date) AS date,b.brent,d.dubai,o.oman FROM (SELECT [date_jalali] AS "
             "date,[price] AS brent FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='نفت برنت') b FULL"
             " OUTER JOIN (SELECT [date_jalali] AS date,[price] AS dubai FROM [nooredenadb].[commodity].[commodities_data]"
             " WHERE commodity='نفت دبی') d ON b.date=d.date FULL OUTER JOIN (SELECT [date_jalali] AS date,[price]"
             " AS oman FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='نفت عمان') o ON COALESCE(b.date"
             ",d.date)=o.date ORDER BY date;")
oil = pd.read_sql(query_oil, db_conn)

oil.fillna(method="ffill", axis=0, inplace=True)
oil["light_oil"] = (((oil["brent"] + oil["dubai"] + oil["oman"])/3)-5) * 0.95
oil["heavy_oil"] = (((oil["brent"] + oil["dubai"] + oil["oman"])/3)-6) * 0.95

commodity_petrol = pd.DataFrame(
    columns=["name", "commodity", "reference"],
    data=[
        ["gasoil_0.05_pg", "نفت گاز _ 0.05", "خلیج فارس پلتس"],
        ["gasoil_0.25_pg", "نفت گاز _ 0.25", "خلیج فارس پلتس"],
        ["gasoil_0.005_pg", "نفت گاز _ 0.005", "خلیج فارس پلتس"],
        ["gasoline_95_sng", "بنزین_95", "سنگاپور پلتس"],
        ["gasoline_92_sng", "بنزین_92", "سنگاپور پلتس"],
        ["hsfo_380_pg", "نفت کوره ـ 380", "خلیج فارس پلتس"],
        ["vb_nrc", "وکیوم باتوم", "شرکت ملی پالایش نرخ دلاری"],
        ["kerosene_pg", "نفت سفید", "خلیج فارس پلتس"],
        ["lubecut_heavy_nrc", "لوبکات سنگین", "شرکت ملی پالایش نرخ دلاری"],
        ["lpg_sng", "LPG پلتس", "فوب سنگاپور"],
        ["naphta_pg", "نفتا", "خلیج فارس پلتس"],
        ["gasoil_0.001_pg", "10ppm _ نفت گاز", "خلیج فارس پلتس"],
        ["gasoline_95_pg", "بنزین_95", "خلیج فارس پلتس"],
        ["gasoline_92_pg", "بنزین_92", "خلیج فارس پلتس"],
        ["condensate_pj", "میعانات گازی", "پارس جنوبی پلتس"],
        ["naphta_lr2_pg", "نفتا LR2", "خلیج فارس پلتس"],
        ["benzene_ch", "بنزن پلتس", "چین پلتس"],
        ["benzene_pg", "بنزن داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["px_se", "پارازایلین پلتس", "جنوب شرق آسیا پلتس"],
        ["px_pg", "پارازایلین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["pbr1202", "PBR1202", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["pbr1220", "PBR1220", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["ox_ch", "آرتوزایلین پلتس", "چین پلتس"],
        ["ox_pg", "آرتوزایلین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["sbr1712", "استایرن بوتادین رابر 1712", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["styren", "استایرن پلتس", "چین پلتس"],
        ["sbr1502", "استایرن بوتادین رابر روشن 1502", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["urea_irfob", "اوره", "فوب ایران"],
        ["butadin", "بوتادین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["butane_pg", "بوتان پلتس", "فوب خلیج فارس"],
        ["polyethylene_light_me", "پلی اتیلن سبک پلتس", "خاورمیانه"],
        ["hsfo_180_pg", "نفت کوره ـ 180", "خلیج فارس پلتس"],
        ["polyethylene_heavy_me", "پلی اتیلن سنگین پلتس", "خاورمیانه"],
        ["ethylene_se", "اتیلن پلتس", "جنوب شرق آسیا پلتس"],
        ["propylene_se", "پروپیلن پلتس", "جنوب شرق آسیا پلتس"],
        ["propane_pdid", "پروپان", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["butene_one_pdid", "بوتن وان داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["urea_brzilcfr", "اوره", "سی اف آر برزیل"],
        ["gasoline_pyrolysis", "بنزین پیرولیز داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["gas", "گاز خوراک", "وزارت نفت"],
        ["panthan_plus_pdid", "نفتا(پنتان پلاس)", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["c3_pdid", "سی تری پلاس داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["lg_pdid", "گاز مایع", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["ammonia_pdid", "آمونیاک داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["vcm_pdid", "وینیل کلراید منومر", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["edc_pdid", "اتیلن دی کلراید", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["meg_ch", "منو اتیلن گلایکول پلتس", "چین پلتس"],
        ["acetic_acid_pdid", "اسید استیک داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
        ["terephthalic_acid_pdid", "اسید ترفتالیک", "دفتر توسعه پتروشیمی نرخ دلاری"]
])

oil_refined = pd.DataFrame(columns=["date"])
for c in tqdm(range(len(commodity_petrol))):
    name, commodity, reference = commodity_petrol.loc[c]
    q = (f"SELECT date_jalali AS date, price AS [{name}] FROM [nooredenadb].[commodity].[commodities_data] "
         f"WHERE commodity='{commodity}' AND reference='{reference}'")
    temp = pd.read_sql(q, db_conn)
    oil_refined = oil_refined.merge(temp, on="date", how="outer")
oil_refined.sort_values(by="date", inplace=True, ascending=True, ignore_index=True)

####################################################################################################

oil_refined["naphta_lr2_pg"] = oil_refined["naphta_lr2_pg"].ffill(inplace=False)
oil_refined["naphta_lr2_pg"] = oil_refined["naphta_lr2_pg"].fillna(
    (oil_refined["naphta_pg"] * 1.0164162998), inplace=False)

result_tmp = pd.read_excel(result_path, sheet_name="APAG Data").replace("None", np.nan, inplace=False)
result_tmp = result_tmp[result_tmp["Unnamed: 0"] == "Gasoil 10 ppm(FOB Persian Gulf)($/barrel)"
             ].T.iloc[1:, :].reset_index(drop=False, inplace=False,)
result_tmp.columns = ["Miladi", "temp"]
result_tmp = result_tmp.merge(dim_date, on="Miladi", how="left").drop(columns=["Miladi"], inplace=False)

oil_refined = oil_refined.merge(result_tmp, on="date", how="left")
oil_refined["gasoil_0.001_pg"].fillna(oil_refined["temp"], inplace=True)
oil_refined.drop(columns=["temp"], inplace=True)

ffill_cols = ["vb_nrc", "lubecut_heavy_nrc", "pbr1220", "pbr1202", "sbr1712", "sbr1502",
              "styren", "butadin", "butene_one_pdid", "lg_pdid", "panthan_plus_pdid", "c3_pdid"]
oil_refined.loc[:, ffill_cols] = oil_refined.loc[:, ffill_cols].ffill(inplace=False)

lpg_missing_data = pd.read_sql(f"SELECT [date_jalali] as date, [price] as temp FROM "
                               f"[nooredenadb].[commodity].[commodities_data] WHERE commodity='گاز مایع'", db_conn)
lpg_missing_data = dim_date[dim_date["date"]<="1401-05-02"][["date"]].merge(
    lpg_missing_data, on="date", how="left").ffill(inplace=False)

oil_refined = oil_refined.merge(lpg_missing_data, on="date", how="left")
oil_refined["lpg_sng"] = oil_refined["lpg_sng"].fillna(oil_refined["temp"], inplace=False)
oil_refined.drop(columns=["temp"], inplace=True)

oil_refined.fillna(method="ffill", inplace=True)

oil_refined = oil_refined[oil_refined["date"] > "1400-09-10"].reset_index(drop=True, inplace=False)
oil_refined.fillna(method="bfill", inplace=True)

####################################################################################################

oil_refined["alpha"] = oil_refined["gasoil_0.25_pg"] / oil_refined["gasoil_0.05_pg"]
oil_refined["octane"] = (oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"]) / 3
oil_refined["gasoline_95_eu5"] = oil_refined["gasoline_95_pg"].copy()

oil_refined["gasoline_91_eu5"] = oil_refined["gasoline_95_eu5"] - (4 * oil_refined["octane"])
oil_refined["gasoline_91_eu4"] = oil_refined["gasoline_95_eu5"] - (5 * oil_refined["octane"])
oil_refined["gasoline_91_eu3"] = oil_refined["gasoline_95_eu5"] - (6 * oil_refined["octane"])
oil_refined["gasoline_91_eu2"] = oil_refined["gasoline_95_eu5"] - (7 * oil_refined["octane"])
oil_refined["gasoline_91_eu1"] = oil_refined["gasoline_95_eu5"] - (8 * oil_refined["octane"])

oil_refined["gasoline_87_eu5"] = oil_refined["gasoline_95_eu5"] - (8 * oil_refined["octane"])
oil_refined["gasoline_87_eu4"] = oil_refined["gasoline_95_eu5"] - (9 * oil_refined["octane"])
oil_refined["gasoline_87_eu3"] = oil_refined["gasoline_95_eu5"] - (10 * oil_refined["octane"])
oil_refined["gasoline_87_eu2"] = oil_refined["gasoline_95_eu5"] - (11 * oil_refined["octane"])
oil_refined["gasoline_87_eu1"] = oil_refined["gasoline_95_eu5"] - (12 * oil_refined["octane"])

oil_refined["hsfo_180_normal"] = oil_refined["hsfo_180_pg"].copy()
oil_refined["hsfo_380_normal"] = oil_refined["hsfo_380_pg"].copy()
oil_refined["hsfo_230_normal"] = oil_refined["hsfo_180_pg"] - 0.35 * (
        oil_refined["hsfo_180_pg"] - oil_refined["hsfo_380_pg"])
oil_refined["hsfo_280_normal"] = oil_refined["hsfo_180_pg"] - 0.6 * (
        oil_refined["hsfo_180_pg"] - oil_refined["hsfo_380_pg"])
oil_refined["hsfo_420_normal"] = oil_refined["hsfo_280_normal"] - 0.12 * (
        oil_refined["hsfo_180_pg"] - oil_refined["hsfo_380_pg"])
oil_refined["hsfo_720_normal"] = oil_refined["hsfo_280_normal"] - (
        oil_refined["hsfo_180_pg"] - oil_refined["hsfo_380_pg"])

oil_refined[
    ["hsfo_180_high_sulfur", "hsfo_380_high_sulfur", "hsfo_230_high_sulfur",
     "hsfo_280_high_sulfur", "hsfo_420_high_sulfur", "hsfo_720_high_sulfur"]
] = oil_refined[
        ["hsfo_180_normal", "hsfo_380_normal", "hsfo_230_normal",
         "hsfo_280_normal", "hsfo_420_normal", "hsfo_720_normal"]] * 0.99

oil_refined["gasoil_0.5_pg"] = oil_refined["gasoil_0.25_pg"] - (
        ((oil_refined["gasoil_0.05_pg"] - oil_refined["gasoil_0.25_pg"]) / 2000) * 2500)
oil_refined["gasoil_10"] = oil_refined["gasoil_0.001_pg"]
oil_refined["gasoil_10_50"] = oil_refined["gasoil_0.005_pg"]
oil_refined["gasoil_50_500"] = oil_refined["gasoil_0.05_pg"]
oil_refined["gasoil_500_2500"] = oil_refined["gasoil_0.25_pg"]
oil_refined["gasoil_2500_5000"] = oil_refined["gasoil_0.25_pg"] * (
        oil_refined["gasoil_0.25_pg"] / oil_refined["gasoil_0.05_pg"])
oil_refined["gasoil_5000"] = oil_refined["gasoil_0.5_pg"] * (
        oil_refined["gasoil_0.25_pg"] / oil_refined["gasoil_0.05_pg"])

####################################################################################################
############################## BANDAR CRACK CALCULATION ############################################
####################################################################################################

oil_refined["GASOIL_bandar"] = (0.63 * oil_refined["gasoil_0.005_pg"]) + (
        0.37 * 0.95 * oil_refined["alpha"] * oil_refined["gasoil_0.25_pg"])

oil_refined["GASOLINE_bandar"] = (0.64 * (oil_refined["gasoline_95_sng"] - (5 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])))+ (
        0.36 * (oil_refined["gasoline_95_sng"] - (2 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])))

oil_refined["HSFO"] = oil_refined["hsfo_380_pg"] * (0.963 * 0.1589873)
oil_refined["VB"] = oil_refined["vb_nrc"] * (1.007 * 0.1589873)
oil_refined["KEROSENE"] = oil_refined["kerosene_pg"] + 1
oil_refined["HEAVYLUBECUT"] = oil_refined["lubecut_heavy_nrc"] * (0.938 * 0.1589873)
oil_refined["HEAVYJETFUEL"] = oil_refined["kerosene_pg"] - 1
oil_refined["LPG"] = oil_refined["lpg_sng"] * (0.564 * 0.1589873)
oil_refined["NAPHTA"] = oil_refined["naphta_pg"] * (0.72 * 158.9873 / 1000)
oil_refined["REFINED_OIL_bandar"] = (oil_refined["GASOIL_bandar"] * 0.341441590160615) +\
                                    (oil_refined["GASOLINE_bandar"] * 0.237078210870903) +\
                                    (oil_refined["HSFO"] * 0.237053507799077) +\
                                    (oil_refined["VB"] * 0.0900039309230667) +\
                                    (oil_refined["KEROSENE"] * 0.0311227722742082) +\
                                    (oil_refined["HEAVYLUBECUT"] * 0.0236845686416851) +\
                                    (oil_refined["HEAVYJETFUEL"] * 0.0107937459227936) +\
                                    (oil_refined["LPG"] * 0.0176201780090371) +\
                                    (oil_refined["NAPHTA"] * 0.00677655452100248)
oil_refined["REFINED_OIL_bandar"] *= 0.978897900940288

####################################################################################################

oil_refined["platformit_bandar_new"] = ((oil_refined["naphta_pg"] + 60) * 0.95) * (0.75 * 0.1589873)
oil_refined["gasoline_bandar_new"] = ((0.119109164 * oil_refined["gasoline_91_eu5"]) +
                                      (0.026905703 * oil_refined["gasoline_91_eu4"]) +
                                      (0.377522272 * oil_refined["gasoline_87_eu3"]) +
                                      (0.387768196 * oil_refined["gasoline_87_eu2"]) +
                                      (0.021441435 * oil_refined["gasoline_87_eu4"]) +
                                      (0.06725323 * oil_refined["platformit_bandar_new"]))
oil_refined["gasoil_bandar_new"] = ((0.842124844 * oil_refined["gasoil_10_50"]) +
                                    (0.01545923 * oil_refined["gasoil_2500_5000"]) +
                                    (0.138994719 * oil_refined["gasoil_5000"]) +
                                    (0.002031105 * oil_refined["gasoil_500_2500"]) +
                                    (0.001390102 * oil_refined["gasoil_50_500"]))
oil_refined["hsfo_bandar_new"] = ((0.002402839 * oil_refined["hsfo_280_normal"]) +
                                  (0.997597161 * oil_refined["hsfo_380_normal"])) * (0.963 * 0.1589873)

query_vacuumbottom_bandar = ("SELECT TEMP1.date,TEMP1.vacuumbottom_bandar_new/TEMP2.price vacuumbottom_bandar_new FROM"
                             " (SELECT REPLACE(date,'/','-') date,CAST(SUM(TotalPrice) * 1000 AS float)/CAST(SUM("
                             "Quantity) AS float) vacuumbottom_bandar_new FROM "
                             "[nooredenadb].[ime].[ime_data_historical] WHERE GoodsName='وکیوم باتوم' AND "
                             "ProducerName='پالایش نفت بندرعباس' AND Quantity>0 GROUP BY date) AS TEMP1 Left JOIN "
                             "(SELECT date_jalali as date, price FROM [nooredenadb].[commodity].[commodities_data] "
                             "where commodity='دلار نیما') AS TEMP2 ON TEMP1.date=TEMP2.date order by date")
vacuumbottom_bandar = pd.read_sql(query_vacuumbottom_bandar, db_conn)
vacuumbottom_bandar = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    vacuumbottom_bandar, on="date", how="left").ffill(inplace=False).sort_values(
    by="date", inplace=False, ignore_index=True)
vacuumbottom_bandar["vacuumbottom_bandar_new"] = vacuumbottom_bandar["vacuumbottom_bandar_new"] * (1.05 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(vacuumbottom_bandar, on="date", how="left")

query_lubecut_bandar = ("SELECT TEMP1.date,TEMP1.lubecut_bandar_new/TEMP2.price lubecut_bandar_new FROM (SELECT "
                        "REPLACE(date,'/','-') date,CAST(SUM(TotalPrice) * 1000 AS float)/CAST(SUM(Quantity) AS float)"
                        " lubecut_bandar_new FROM [nooredenadb].[ime].[ime_data_historical] WHERE GoodsName IN"
                        " ('لوب کات سبک', 'لوب کات سنگین') AND ProducerName='پالایش نفت بندرعباس' AND Quantity>0 "
                        "GROUP BY date) AS TEMP1 Left JOIN (SELECT date_jalali as date,price FROM [nooredenadb]."
                        "[commodity].[commodities_data] where commodity='دلار نیما') AS TEMP2 ON TEMP1.date=TEMP2.date"
                        " order by date")
lubecut_bandar = pd.read_sql(query_lubecut_bandar, db_conn)
lubecut_bandar = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    lubecut_bandar, on="date", how="left").ffill(inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
lubecut_bandar["lubecut_bandar_new"] = lubecut_bandar["lubecut_bandar_new"] * (0.94 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(lubecut_bandar, on="date", how="left")

oil_refined["kerosene_bandar_new"] = oil_refined["kerosene_pg"].copy()
oil_refined["jetfuel_bandar_new"] = oil_refined["kerosene_bandar_new"] + 1
oil_refined["lpg_bandar_new"] = (oil_refined["lpg_sng"] * 0.912408547094171) * (0.55 * 0.1589873)
oil_refined["naphta_bandar_new"] = oil_refined["naphta_pg"] * (0.77 * 0.1589873)

oil_refined["REFINED_OIL_bandar_new"] = ((oil_refined["gasoline_bandar_new"] * 0.227307029) +
                                         (oil_refined["gasoil_bandar_new"] * 0.350810236) +
                                         (oil_refined["hsfo_bandar_new"] * 0.214586182) +
                                         (oil_refined["vacuumbottom_bandar_new"] * 0.122644535) +
                                         (oil_refined["lubecut_bandar_new"] * 0.026150806) +
                                         (oil_refined["kerosene_bandar_new"] * 0.009224354) +
                                         (oil_refined["jetfuel_bandar_new"] * 0.013781095) +
                                         (oil_refined["lpg_bandar_new"] * 0.018862769) +
                                         (oil_refined["naphta_bandar_new"] * 0.007061987))
oil_refined["REFINED_OIL_bandar_new"] *= 0.978897900940288

####################################################################################################
############################## SHPNA CRACK CALCULATION #############################################
####################################################################################################

oil_refined["GASOIL_shpna"] = (0.6 * oil_refined["gasoil_0.005_pg"]) + (
        0.4 * 0.95 * oil_refined["alpha"] * oil_refined["gasoil_0.25_pg"])
oil_refined["GASOLINE_shpna"] = (oil_refined["gasoline_95_sng"] - (4 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"]))
oil_refined["GASOLINE_shpna_new"] = (oil_refined["gasoline_95_pg"] - (8 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"]))

oil_refined["ISORECYCLE"] = 1.34 * oil_refined["HEAVYLUBECUT"]
oil_refined["RAWOIL"] = 0.8 * oil_refined["ISORECYCLE"]
oil_refined["REFINED_OIL_shpna"] = (oil_refined["GASOIL_shpna"] * 0.420016747750079) +\
                                    (oil_refined["GASOLINE_shpna"] * 0.214414536129383) +\
                                    (oil_refined["HSFO"] * 0.117926155859682) +\
                                    (oil_refined["VB"] * 0.111513104049267) +\
                                    (oil_refined["KEROSENE"] * 0.0100810603485273) +\
                                    (oil_refined["ISORECYCLE"] * 0.0110861438496628) +\
                                    (oil_refined["RAWOIL"] * 0.0310253982051612) +\
                                    (oil_refined["HEAVYJETFUEL"] * 0.0104068020846743) +\
                                    (oil_refined["LPG"] * 0.02438620260699) +\
                                    (oil_refined["NAPHTA"] * 0.0124706827885459)
oil_refined["REFINED_OIL_shpna"] *= 0.984627611564425

####################################################################################################

oil_refined["gasoline_shpna_new"] = ((0.0200581502509881 * oil_refined["gasoline_87_eu4"]) +
                                     (0.976023998307571 * oil_refined["gasoline_91_eu5"]) +
                                     (0.00391785144144108 * oil_refined["gasoline_95_eu5"]))

oil_refined["gasoil_shpna_new"] = ((0.618457198377634 * oil_refined["gasoil_10"]) +
                                   (0.256924041090112 * oil_refined["gasoil_10_50"]) +
                                   (0.0360324027878016 * oil_refined["gasoil_2500_5000"]) +
                                   (0.0462890959883476 * oil_refined["gasoil_500_2500"]) +
                                   (0.0194237596526852 * oil_refined["gasoil_50_500"]) +
                                   (0.02287350210342 * oil_refined["gasoil_5000"]))

oil_refined["hsfo_shpna_new"] = ((0.0458599521931236 * oil_refined["hsfo_230_normal"]) +
                                 (0.0164487083996289 * oil_refined["hsfo_280_normal"]) +
                                 (0.937691339407248 * oil_refined["hsfo_380_normal"])) * (0.963 * 0.1589873)

query_vacuumbottom_shpna = ("SELECT TEMP1.date,TEMP1.vacuumbottom_shpna_new/TEMP2.price vacuumbottom_shpna_new FROM"
                            " (SELECT REPLACE(date,'/','-') date,CAST(SUM(TotalPrice) * 1000 AS float)/"
                            "CAST(SUM(Quantity) AS float) vacuumbottom_shpna_new FROM "
                            "[nooredenadb].[ime].[ime_data_historical] WHERE GoodsName='وکیوم باتوم' AND "
                            "ProducerName='پالایش نفت اصفهان' AND Quantity>0 GROUP BY date) AS TEMP1 Left JOIN "
                            "(SELECT date_jalali as date, price FROM [nooredenadb].[commodity].[commodities_data] "
                            "where commodity='دلار نیما') AS TEMP2 ON TEMP1.date=TEMP2.date order by date")
vacuumbottom_shpna = pd.read_sql(query_vacuumbottom_shpna, db_conn)
vacuumbottom_shpna = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    vacuumbottom_shpna, on="date", how="left").ffill(
    inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
vacuumbottom_shpna["vacuumbottom_shpna_new"] *= (1.05 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(vacuumbottom_shpna, on="date", how="left")

query_lubecut_shpna = ("SELECT TEMP1.date,TEMP1.lubecut_shpna_new/TEMP2.price lubecut_shpna_new FROM (SELECT "
                       "REPLACE(date,'/','-') date,CAST(SUM(TotalPrice) * 1000 AS float)/CAST(SUM(Quantity) AS "
                       "float) lubecut_shpna_new FROM [nooredenadb].[ime].[ime_data_historical] WHERE GoodsName "
                       "IN ('لوب کات سبک', 'لوب کات سنگین') AND ProducerName='پالایش نفت اصفهان' AND Quantity>0 "
                       "GROUP BY date) AS TEMP1 Left JOIN (SELECT date_jalali as date,price FROM [nooredenadb]."
                       "[commodity].[commodities_data] where commodity='دلار نیما') AS TEMP2 ON TEMP1.date=TEMP2.date "
                       "order by date")
lubecut_shpna = pd.read_sql(query_lubecut_shpna, db_conn)
lubecut_shpna = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    lubecut_shpna, on="date", how="left").ffill(inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
lubecut_shpna["lubecut_shpna_new"] *= (0.94 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(lubecut_shpna, on="date", how="left")

oil_refined["lpg_shpna_new"] = (oil_refined["lpg_sng"] * 0.912408547094171)
oil_refined["lpg_shpna_new"] *= (0.55 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined["kerosene_shpna_new"] = oil_refined["kerosene_pg"].copy()
oil_refined["naphta_shpna_new"] = oil_refined["naphta_pg"] * (0.77 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined["jetfuel_shpna_new"] = oil_refined["kerosene_shpna_new"] + 1

query_isorecycle_shpna = ("SELECT TEMP1.date, TEMP1.isorecycle_shpna_new/TEMP2.price AS isorecycle_shpna_new FROM "
                          "(SELECT REPLACE(tradeDateShamsi, '/', '-') AS date, CAST(SUM(totalValueRialsTooltip) AS "
                          "float) / CAST(SUM(totTradedQuantity) AS float) AS isorecycle_shpna_new FROM "
                          "[nooredenadb].[iee].[iee_data_historical] WHERE supplierName='شركت پالایش نفت اصفهان' AND "
                          "instrumentDisplayName='آیزوریسایکل' AND totTradedQuantity>0 GROUP BY tradeDateShamsi) AS "
                          "TEMP1 Left JOIN (SELECT date_jalali date, price FROM [nooredenadb].[commodity].[commodities_data]"
                          " where commodity='دلار نیما') TEMP2 ON TEMP1.date=TEMP2.date order by date")
isorecycle_shpna = pd.read_sql(query_isorecycle_shpna, db_conn)
isorecycle_shpna = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    isorecycle_shpna, on="date", how="left").ffill(
    inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
isorecycle_shpna["isorecycle_shpna_new"] *= (0.851195739136834 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(isorecycle_shpna, on="date", how="left")

query_isofeed_shpna = ("SELECT TEMP1.date, TEMP1.isofeed_shpna_new/TEMP2.price AS isofeed_shpna_new FROM "
                          "(SELECT REPLACE(tradeDateShamsi, '/', '-') AS date, CAST(SUM(totalValueRialsTooltip) AS "
                          "float) / CAST(SUM(totTradedQuantity) AS float) AS isofeed_shpna_new FROM "
                          "[nooredenadb].[iee].[iee_data_historical] WHERE supplierName='شركت پالایش نفت اصفهان' AND "
                          "instrumentDisplayName='آیزوفید' AND totTradedQuantity>0 GROUP BY tradeDateShamsi) AS "
                          "TEMP1 Left JOIN (SELECT date_jalali date, price FROM [nooredenadb].[commodity].[commodities_data]"
                          " where commodity='دلار نیما') TEMP2 ON TEMP1.date=TEMP2.date order by date")
isofeed_shpna = pd.read_sql(query_isofeed_shpna, db_conn)
isofeed_shpna = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    isofeed_shpna, on="date", how="left").ffill(inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
isofeed_shpna["isofeed_shpna_new"] *= (0.832639877367787 * 0.1589873)  # change unit from (dollar/ton) to (dollar/barrel)
oil_refined = oil_refined.merge(isofeed_shpna, on="date", how="left")

query_solvent_shpna = ("SELECT TEMP1.date, TEMP1.solvent_shpna_new/TEMP2.price AS solvent_shpna_new FROM "
                       "(SELECT REPLACE(tradeDateShamsi, '/', '-') AS date, CAST(SUM(totalValueRialsTooltip) AS "
                       "float) / CAST(SUM(totTradedQuantity) AS float) AS solvent_shpna_new FROM "
                       "[nooredenadb].[iee].[iee_data_historical] WHERE supplierName='شركت پالایش نفت اصفهان' AND "
                       "instrumentDisplayName IN ('حلال 402', 'حلال 502', 'حلال 400', 'حلال 406', 'حلال 503', 'حلال 410') "
                       "AND priceUnitTitle='ریال' AND totTradedQuantity>0 GROUP BY tradeDateShamsi) AS TEMP1 Left JOIN "
                       "(SELECT date_jalali date, price FROM [nooredenadb].[commodity].[commodities_data] where "
                       "commodity='دلار نیما') TEMP2 ON TEMP1.date=TEMP2.date order by date")
solvent_shpna = pd.read_sql(query_solvent_shpna, db_conn)
solvent_shpna = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    solvent_shpna, on="date", how="left").ffill(inplace=False).sort_values(by="date", inplace=False, ignore_index=True)
solvent_shpna["solvent_shpna_new"] *= (158.9873)  # change unit from (dollar/liter) to (dollar/barrel)
oil_refined = oil_refined.merge(solvent_shpna, on="date", how="left")

oil_refined["aromatic_shpna_new"] = oil_refined["kerosene_shpna_new"] * 0.948935704618041
oil_refined["hydrocarbora_shpna_new"] = oil_refined["kerosene_shpna_new"] * 0.874900718741564


oil_refined["REFINED_OIL_shpna_new"] = ((oil_refined["gasoline_shpna_new"] * 0.204530655656214) +
                                        (oil_refined["gasoil_shpna_new"] * 0.388198350458766) +
                                        (oil_refined["hsfo_shpna_new"] * 0.149132817397275) +
                                        (oil_refined["vacuumbottom_shpna_new"] * 0.109383688508203) +
                                        (oil_refined["lubecut_shpna_new"] * 0.0309723474235829) +
                                        (oil_refined["lpg_shpna_new"] * 0.0316339196574885) +
                                        (oil_refined["kerosene_shpna_new"] * 0.0204616761856519) +
                                        (oil_refined["naphta_shpna_new"] * 0.010265126897632) +
                                        (oil_refined["jetfuel_shpna_new"] * 0.00974945016652881) +
                                        (oil_refined["isorecycle_shpna_new"] * 0.00890726141318956) +
                                        (oil_refined["isofeed_shpna_new"] * 0.00387256993103292) +
                                        (oil_refined["solvent_shpna_new"] * 0.013496822738837) +
                                        (oil_refined["aromatic_shpna_new"] * 0.00902319022943869) +
                                        (oil_refined["hydrocarbora_shpna_new"] * 0.00799495829708157))
oil_refined["REFINED_OIL_shpna_new"] *= 0.984627611564425

####################################################################################################
############################## SHAVAN CRACK CALCULATION ############################################
####################################################################################################

oil_refined["GASOIL_shavan"] = oil_refined["gasoil_0.005_pg"]

oil_refined["GASOLINE_shavan"] = oil_refined["gasoline_95_sng"] - (5 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])
oil_refined["GASOLINE_shavan_new"] = oil_refined["gasoline_95_pg"] - (9 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])

oil_refined["NAPHTA_shavan"] = oil_refined["naphta_lr2_pg"] * 0.72 * 0.1589873
oil_refined["condensate_shavan"] = (oil_refined["condensate_pj"]-2) * 0.95
oil_refined["REFINED_OIL_shavan"] = (oil_refined["HSFO"] * 0.208651351) +\
                                    (oil_refined["gasoil_0.005_pg"] * 0.296821798) +\
                                    (oil_refined["GASOLINE_shavan"] * 0.349211287) +\
                                    (oil_refined["NAPHTA_shavan"] * 0.133005252)
oil_refined["REFINED_OIL_shavan_new"] = (oil_refined["HSFO"] * 0.208651351) +\
                                    (oil_refined["gasoil_0.005_pg"] * 0.296821798) +\
                                    (oil_refined["GASOLINE_shavan_new"] * 0.349211287) +\
                                    (oil_refined["NAPHTA_shavan"] * 0.133005252)

####################################################################################################
############################## SHETRAN CRACK CALCULATION ###########################################
####################################################################################################

oil_refined["GASOLINE_shetran"] = oil_refined["gasoline_95_sng"] - (5 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])
oil_refined["GASOLINE_shetran_new"] = oil_refined["gasoline_95_pg"] - (9 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])
oil_refined["REFINED_OIL_shetran"] = (oil_refined["VB"] * 0.064179605) + (oil_refined["LPG"] * 0.029584294) + (
        oil_refined["NAPHTA"] * 0.010647009) + (oil_refined["HSFO"] * 0.201931825) + (
        oil_refined["HEAVYJETFUEL"] * 0.058767085) + (oil_refined["ISORECYCLE"] * 0.067717507) + (
        oil_refined["GASOLINE_shetran"] * 0.172272832) + (oil_refined["gasoil_0.25_pg"] * 0.390201004)
oil_refined["REFINED_OIL_shetran_new"] = (oil_refined["VB"] * 0.064179605) + (oil_refined["LPG"] * 0.029584294) + (
        oil_refined["NAPHTA"] * 0.010647009) + (oil_refined["HSFO"] * 0.201931825) + (
        oil_refined["HEAVYJETFUEL"] * 0.058767085) + (oil_refined["ISORECYCLE"] * 0.067717507) + (
        oil_refined["GASOLINE_shetran_new"] * 0.172272832) + (oil_refined["gasoil_0.25_pg"] * 0.390201004)

####################################################################################################

oil_refined = oil_refined.merge(oil[["date", "light_oil", "heavy_oil"]], on="date", how="left")
oil_refined.fillna(method="ffill", inplace=True)

oil_refined["کرک شبندر قدیم"] = oil_refined["REFINED_OIL_bandar"] - (
        (oil_refined["heavy_oil"] * 0.930180572983039) + (oil_refined["condensate_shavan"] * 0.0698194270169608))
oil_refined["کرک شبندر جدید"] = oil_refined["REFINED_OIL_bandar_new"] - (
        (oil_refined["heavy_oil"] * 0.930180572983039) + (oil_refined["condensate_shavan"] * 0.0698194270169608))

oil_refined["کرک شپنا قدیم"] = oil_refined["REFINED_OIL_shpna"] - oil_refined["light_oil"]
oil_refined["کرک شپنا جدید"] = oil_refined["REFINED_OIL_shpna_new"] - oil_refined["light_oil"]

oil_refined["کرک شاوان قدیم"] = oil_refined["REFINED_OIL_shavan"] - (
        (oil_refined["condensate_shavan"] * 0.425465269057474) + (oil_refined["light_oil"] * 0.574534730942526))
oil_refined["کرک شاوان جدید"] = oil_refined["REFINED_OIL_shavan_new"] - (
        (oil_refined["condensate_shavan"] * 0.425465269057474) + (oil_refined["light_oil"] * 0.574534730942526))

oil_refined["کرک شتران قدیم"] = oil_refined["REFINED_OIL_shetran"] - oil_refined["light_oil"]
oil_refined["کرک شتران جدید"] = oil_refined["REFINED_OIL_shetran_new"] - oil_refined["light_oil"]

####################################################################################################
############################## SHGHADIR CRACK CALCULATION #########################################
####################################################################################################

oil_refined["VCM_shghadir"] = oil_refined["vcm_pdid"] * 0.679753260620776
oil_refined["CHLORINE_shghadir"] = 121
oil_refined["EDC_shghadir"] = oil_refined["edc_pdid"] * 1.05311922411115
oil_refined["ETHYLENE_shghadir"] = oil_refined["ethylene_se"] - 250
oil_refined["RAW_MATERIAL_shghadir"] = ((oil_refined["VCM_shghadir"] * 0.0229857747773831) +
                                        (oil_refined["CHLORINE_shghadir"] * 0.483084519642742) +
                                        (oil_refined["EDC_shghadir"] * 0.397625051418869) +
                                        (oil_refined["ETHYLENE_shghadir"] * 0.322586969924535))

query_pvc_shghadir = ("SELECT TEMP1.date,TEMP1.PVC_shghadir/TEMP2.price PVC_shghadir FROM (SELECT REPLACE(date,'/','-')"
                      " date,CAST(SUM(TotalPrice) * 1000 AS float)/CAST(SUM(Quantity) AS float) PVC_shghadir FROM "
                      "[nooredenadb].[ime].[ime_data_historical] WHERE ProducerName='پتروشیمی غدیر' AND Quantity > 0 "
                      "GROUP BY date) AS TEMP1 Left JOIN (SELECT date_jalali as date,price FROM [nooredenadb].[commodity]."
                      "[commodities_data] where commodity='دلار نیما') AS TEMP2 ON TEMP1.date=TEMP2.date order by date")
pvc_shghadir = pd.read_sql(query_pvc_shghadir, db_conn)
pvc_shghadir = dim_date[dim_date["date"]>="1400-01-01"][["date"]].merge(
    pvc_shghadir, on="date", how="left").sort_values(by="date", inplace=False, ignore_index=True).ffill(inplace=False)
oil_refined = oil_refined.merge(pvc_shghadir, on="date", how="left")
oil_refined["FINAL_PRODUCT_shghadir"] = oil_refined["PVC_shghadir"].copy()

oil_refined["کرک شغدیر"] = oil_refined["FINAL_PRODUCT_shghadir"] - oil_refined["RAW_MATERIAL_shghadir"]

####################################################################################################
############################## SHGOUYA CRACK CALCULATION ###########################################
####################################################################################################

oil_refined["MEG_shgouya"] = oil_refined["meg_ch"] * 0.928320200217231
oil_refined["ACETIC_ACID_shgouya"] = oil_refined["acetic_acid_pdid"] * 1.0180104198081
oil_refined["PX_shgouya"] = oil_refined["px_se"] * 0.936156381175639
oil_refined["HYDROGEN_shgouya"] = 14
oil_refined["TEREPHTHALIC_ACID_shgouya"] = oil_refined["terephthalic_acid_pdid"].copy()

oil_refined["RAW_MATERIAL_shgouya"] = ((oil_refined["MEG_shgouya"] * 0.330911035413314) +
                                       (oil_refined["TEREPHTHALIC_ACID_shgouya"] * 0.00167636870292642) +
                                       (oil_refined["ACETIC_ACID_shgouya"] * 0.0572225788739768) +
                                       (oil_refined["PX_shgouya"] * 0.577284148045197) +
                                       (oil_refined["HYDROGEN_shgouya"] * 0.00122029289265118))

query_products_shgouya = ("SELECT REPLACE(date, '/', '-') AS date, TotalPrice, Quantity FROM "
                          "[nooredenadb].[ime].[ime_data_historical] WHERE "
                          "ProducerName = 'پتروشیمی تندگویان' AND Quantity > 0")
query_dollar_nima = ("SELECT date_jalali as date, price AS dollar_nima FROM "
                     "[nooredenadb].[commodity].[commodities_data] where commodity='دلار نیما'")
query_weekends = ("SELECT REPLACE(weekend, '/', '-') AS weekend, REPLACE(date, '/', '-') AS date FROM (SELECT "
                  "MAX(Jalali_1) as weekend, jyear, JWeekNum FROM [nooredenadb].[extra].[dim_date] GROUP BY jyear, "
                  "JWeekNum) AS TEMP1 RIGHT JOIN (SELECT Jalali_1 AS date, jyear, JWeekNum FROM [nooredenadb].[extra]."
                  "[dim_date]) AS TEMP2 ON TEMP1.jyear=TEMP2.jyear AND TEMP1.JWeekNum=TEMP2.JWeekNum")

products_shgouya = pd.read_sql(query_products_shgouya, db_conn)
dollar_nima = pd.read_sql(query_dollar_nima, db_conn)
weekends = pd.read_sql(query_weekends, db_conn)
dollar_nima = dollar_nima.merge(weekends[["date"]], on="date", how="outer").ffill(inplace=False)
products_shgouya = products_shgouya.groupby(by="date", as_index=False).sum()
products_shgouya = products_shgouya.merge(dollar_nima, on="date", how="left").dropna(subset=["dollar_nima"], inplace=False)

products_shgouya["TotalPrice"] = (1000 * products_shgouya["TotalPrice"]) / products_shgouya["dollar_nima"]

products_shgouya_ = products_shgouya.merge(weekends, on="date", how="left")

products_shgouya__ = products_shgouya_.groupby(
    by="weekend", as_index=False).sum()[["weekend", "TotalPrice", "Quantity"]]
products_shgouya__["FINAL_PRODUCT_shgouya"] = products_shgouya__["TotalPrice"] / products_shgouya__["Quantity"]
products_shgouya___ = products_shgouya_[["date", "weekend"]].merge(
    products_shgouya__[["weekend", "FINAL_PRODUCT_shgouya"]])[["date", "FINAL_PRODUCT_shgouya"]]

oil_refined = oil_refined.merge(products_shgouya___[["date", "FINAL_PRODUCT_shgouya"]], on="date", how="left")
oil_refined["کرک شگویا"] = oil_refined["FINAL_PRODUCT_shgouya"] - oil_refined["RAW_MATERIAL_shgouya"]

####################################################################################################
############################## NOORI CRACK CALCULATION #############################################
####################################################################################################

oil_refined["HEAVYCUT_DOMESTIC_noori"] = ((1.03982712451197 * oil_refined["gasoil_0.25_pg"] * 1000) / 158.9873) / 0.844
oil_refined["HEAVYCUT_EXPORT_noori"] = oil_refined["HEAVYCUT_DOMESTIC_noori"] * 0.940043722693393
oil_refined["BENZENE_noori"] = oil_refined["benzene_pg"] * 0.939419190745448
oil_refined["OX_DOMESTIC_noori"] = oil_refined["ox_pg"] * 0.925848407352329
oil_refined["OX_EXPORT_noori"] = oil_refined["OX_DOMESTIC_noori"] * 0.940043722693393
oil_refined["LIGHTCUT_noori"] = oil_refined["naphta_pg"] * 0.897452656153756
oil_refined["AROMATHIC_HEAVY_noori"] = oil_refined["naphta_pg"] * 1.06582341438451
oil_refined["PX_noori"] = oil_refined["px_pg"] * 0.859012669618298
oil_refined["RAFFINATE_DOMESTIC_noori"] = oil_refined["naphta_pg"] * 0.938582178119477
oil_refined["RAFFINATE_EXPORT_noori"] = oil_refined["RAFFINATE_DOMESTIC_noori"] * 1.05251909910674
oil_refined["LG_noori"] = oil_refined["lg_pdid"] * 0.937467165354759
oil_refined["C5CUT_noori"] = oil_refined["panthan_plus_pdid"] * 1.06023615986744
oil_refined["LIGHTNAPHTA_noori"] = oil_refined["naphta_pg"] * 0.973343855187436
oil_refined["GASOLINE_87_noori"] = (((oil_refined["gasoline_95_pg"] -
                                      (((oil_refined["gasoline_95_pg"] -
                                         oil_refined["gasoline_92_pg"]) / 3) * 8)) * 1000) / 158.9873) / 0.6453

oil_refined["A80_A92_OLD_noori"] = (oil_refined["GASOLINE_87_noori"] * 2.5) - (oil_refined["naphta_pg"] * 1.5)

oil_refined["A80_A92_NEW_noori"] = ((oil_refined["naphta_pg"] * 0.23) + (oil_refined["ox_pg"] * 0.07) +
                                    (oil_refined["px_pg"] * 0.7) + 50)
oil_refined["A80_A92_NEW_noori"] = (((oil_refined["date"] >= "1403-08-06") * oil_refined["A80_A92_NEW_noori"]) +
                                    ((~(oil_refined["date"] >= "1403-08-06")) * oil_refined["A80_A92_OLD_noori"]))

oil_refined["GASOLINE_noori"] = oil_refined["naphta_pg"] * 0.850302481823993
oil_refined["BENZENE_TOLUENE_noori"] = oil_refined["benzene_pg"] * 0.902067230182237
oil_refined["CONDENSATE_noori"] = (((oil_refined["condensate_pj"] * 1000) / 158.9873) / 0.7345) * 0.806316
oil_refined["GASOLINE_PYROLYSIS_noori"] = oil_refined["gasoline_pyrolysis"] * 0.960832106775624
oil_refined["OTHER_noori"] = 1116
oil_refined["RAW_MATERIAL_noori"] = ((oil_refined["GASOLINE_noori"] * 0.00605969083258019) +
                                     (oil_refined["BENZENE_TOLUENE_noori"] * 0.00343213265708825) +
                                     (oil_refined["CONDENSATE_noori"] * 0.942319985768347) +
                                     (oil_refined["GASOLINE_PYROLYSIS_noori"] * 0.0481708770048567) +
                                     (oil_refined["OTHER_noori"] * 0.0000173137371278421))



oil_refined["FINAL_PRODUCT_noori_OLD"] = ((oil_refined["HEAVYCUT_DOMESTIC_noori"] * 0.0872669408496597) +
                                          (oil_refined["HEAVYCUT_EXPORT_noori"] * 0.176889984814069) +
                                          (oil_refined["BENZENE_noori"] * 0.0822768132360625) +
                                          (oil_refined["OX_DOMESTIC_noori"] * 0.00760843031942617) +
                                          (oil_refined["OX_EXPORT_noori"] * 0.000662270331828441) +
                                          (oil_refined["LIGHTCUT_noori"] * 0.137338692841592) +
                                          (oil_refined["AROMATHIC_HEAVY_noori"] * 0.00539388800620378) +
                                          (oil_refined["PX_noori"] * 0.0185865556634557) +
                                          (oil_refined["RAFFINATE_DOMESTIC_noori"] * 0.0406459143126945) +
                                          (oil_refined["RAFFINATE_EXPORT_noori"] * 0.0647152825805992) +
                                          (oil_refined["LG_noori"] * 0.0284401469602331) +
                                          (oil_refined["C5CUT_noori"] * 0.00538783374194205) +
                                          (oil_refined["LIGHTNAPHTA_noori"] * 0.057729854146912) +
                                          (oil_refined["A80_A92_OLD_noori"] * 0.287057392195322))
oil_refined["FINAL_PRODUCT_noori_NEW"] = ((oil_refined["HEAVYCUT_DOMESTIC_noori"] * 0.0872669408496597) +
                                          (oil_refined["HEAVYCUT_EXPORT_noori"] * 0.176889984814069) +
                                          (oil_refined["BENZENE_noori"] * 0.0822768132360625) +
                                          (oil_refined["OX_DOMESTIC_noori"] * 0.00760843031942617) +
                                          (oil_refined["OX_EXPORT_noori"] * 0.000662270331828441) +
                                          (oil_refined["LIGHTCUT_noori"] * 0.137338692841592) +
                                          (oil_refined["AROMATHIC_HEAVY_noori"] * 0.00539388800620378) +
                                          (oil_refined["PX_noori"] * 0.0185865556634557) +
                                          (oil_refined["RAFFINATE_DOMESTIC_noori"] * 0.0406459143126945) +
                                          (oil_refined["RAFFINATE_EXPORT_noori"] * 0.0647152825805992) +
                                          (oil_refined["LG_noori"] * 0.0284401469602331) +
                                          (oil_refined["C5CUT_noori"] * 0.00538783374194205) +
                                          (oil_refined["LIGHTNAPHTA_noori"] * 0.057729854146912) +
                                          (oil_refined["A80_A92_NEW_noori"] * 0.287057392195322))

oil_refined["کرک نوری قدیم"] = oil_refined["FINAL_PRODUCT_noori_OLD"] - oil_refined["RAW_MATERIAL_noori"]
oil_refined["کرک نوری جدید"] = oil_refined["FINAL_PRODUCT_noori_NEW"] - oil_refined["RAW_MATERIAL_noori"]

####################################################################################################
############################## SHJAM CRACK CALCULATION #############################################
####################################################################################################

oil_refined["PBR_shjam"] = (((oil_refined["pbr1202"] + oil_refined["pbr1220"]) / 2) * 0.92 * 0.43) + (
        ((oil_refined["pbr1202"] + oil_refined["pbr1220"]) / 2) * 0.92 * 0.96 * 0.57)
oil_refined["SBR_shjam"] = (((oil_refined["sbr1712"] + oil_refined["sbr1502"]) / 2) * 0.95 * 0.91) + (
        ((oil_refined["sbr1712"] + oil_refined["sbr1502"]) / 2) * 0.95 * 0.96 * 0.09)
oil_refined["FINAL_PRODUCT_shjam"] = (oil_refined["PBR_shjam"] * 0.562) + (oil_refined["SBR_shjam"] * 0.438)
oil_refined["RAW_MATERIAL_shjam"] = ((oil_refined["butadin"] * 0.79) + (oil_refined["styren"] * 0.18 * 0.44))/0.92
oil_refined["کرک شجم"] = oil_refined["FINAL_PRODUCT_shjam"] - oil_refined["RAW_MATERIAL_shjam"]

####################################################################################################
############################## JAM CRACK CALCULATION ###############################################
####################################################################################################

oil_refined["POLYETHYLENE_HEAVY_jam"] = (oil_refined["polyethylene_heavy_me"] * 0.27) + (
        oil_refined["polyethylene_heavy_me"] * 0.95 * 0.73)
oil_refined["POLYETHYLENE_LIGHT_jam"] = (oil_refined["polyethylene_light_me"] * 0.54) + (
        oil_refined["polyethylene_light_me"] * 0.95 * 0.46)
oil_refined["ETHYLENE_jam"] = (oil_refined["ethylene_se"] * 0.95) - 180
oil_refined["BENZENE_PIROLIZ_jam"] = (oil_refined["naphta_pg"] - 13) * 0.95
oil_refined["PROPYLENE_jam"] = (oil_refined["propylene_se"] - 200) * 0.95
oil_refined["BUTADINE_jam"] = oil_refined["butadin"]
oil_refined["BUTENE_ONE_jam"] = oil_refined["butene_one_pdid"]
oil_refined["ETC_jam"] = oil_refined["butadin"] * 0.8
oil_refined["HSFO_jam"] = oil_refined["hsfo_180_pg"]
oil_refined["PROPANE_jam"] = oil_refined["propane_pdid"]
oil_refined["RAFFINATE_jam"] = oil_refined["naphta_pg"] * 0.97
oil_refined["LIGHT_CUT_jam"] = oil_refined["naphta_pg"] * 0.965 * 0.95
oil_refined["AROMATIC_HEAVY_jam"] = oil_refined["px_se"] - 320
oil_refined["PANTHANE_CUT__jam"] = oil_refined["naphta_pg"] * 0.95
oil_refined["ETC_jam"] = oil_refined["PANTHANE_CUT__jam"] * 2.7
oil_refined["LG_jam"] = oil_refined["lg_pdid"]
oil_refined["PANTHANE_PLUS_jam"] = oil_refined["panthan_plus_pdid"]
oil_refined["c3_PLUS_jam"] = oil_refined["c3_pdid"]
oil_refined["BUTANE_jam"] = oil_refined["butane_pg"] * 0.95
oil_refined["ETHANE_jam"] = ((((oil_refined["polyethylene_heavy_me"] + oil_refined["polyethylene_light_me"]) / 2) +
                              oil_refined["naphta_pg"] - 32) * 0.25) - 145
oil_refined["ETHANE_jam"] = (((oil_refined["ETHANE_jam"] >= 400) * 400) +
                             ((oil_refined["ETHANE_jam"] <= 220) * 220) +
                             (((oil_refined["ETHANE_jam"] < 400) & (oil_refined["ETHANE_jam"] > 220))
                              * oil_refined["ETHANE_jam"]))
oil_refined["FINAL_PRODUCT_jam"] = (oil_refined["POLYETHYLENE_HEAVY_jam"] * 0.201744404952462) +\
                                   (oil_refined["POLYETHYLENE_LIGHT_jam"] * 0.212265909449066) +\
                                   (oil_refined["ETHYLENE_jam"] * 0.235482971445266) +\
                                   (oil_refined["BENZENE_PIROLIZ_jam"] * 0.155608671476855) +\
                                   (oil_refined["PROPYLENE_jam"] * 0.127745323025701) +\
                                   (oil_refined["BUTADINE_jam"] * 0.0478127347626896) +\
                                   (oil_refined["BUTENE_ONE_jam"] * 0.00579529384555046) +\
                                   (oil_refined["ETC_jam"] * 0.00317363520997228) +\
                                   (oil_refined["HSFO_jam"] * 0.0103710558324388)
oil_refined["RAW_MATERIAL_jam"] = (oil_refined["PROPANE_jam"] * 0.00147041945275248) +\
                                  (oil_refined["RAFFINATE_jam"] * 0.0300357865503664) +\
                                  (oil_refined["LIGHT_CUT_jam"] * 0.344209700480608) +\
                                  (oil_refined["AROMATIC_HEAVY_jam"] * 0.00781736623817153) +\
                                  (oil_refined["PANTHANE_CUT__jam"] * 0.0222898236079147) +\
                                  (oil_refined["ETC_jam"] * 4.21089001553784E-06) +\
                                  (oil_refined["ETHANE_jam"] * 0.445525930723375) +\
                                  (oil_refined["LG_jam"] * 0.0157055656424206) +\
                                  (oil_refined["PANTHANE_PLUS_jam"] * 0.00434209097677931) +\
                                  (oil_refined["c3_PLUS_jam"] * 0.109928161970668) +\
                                  (oil_refined["BUTANE_jam"] * 0.0186709434669279)
oil_refined["کرک جم"] = oil_refined["FINAL_PRODUCT_jam"] - oil_refined["RAW_MATERIAL_jam"]

####################################################################################################
############################## SHAPDIS CRACK CALCULATION ###########################################
####################################################################################################

oil_refined["UREA_AGRIC__shapdis"] = oil_refined["urea_irfob"] * 0.75
oil_refined["AMMONIA_shapdis"] = oil_refined["ammonia_pdid"]
oil_refined["UREA_INDUS__shapdis"] = (oil_refined["urea_brzilcfr"] - 35) * 0.9
oil_refined["GAS_shapdis"] = ((((oil_refined["date"] >= "1400-09-01") &
                                (oil_refined["date"] <="1401-12-30") &
                                (oil_refined["gas"] > 0.2)) * 0.2) +
                              (((oil_refined["date"] < "1400-09-01") |
                                (oil_refined["date"] > "1401-12-30") |
                                (oil_refined["gas"] <= 0.2)) * oil_refined["gas"]))

oil_refined["FINAL_PRODUCT_shapdis"] = (oil_refined["UREA_AGRIC__shapdis"] * 0.215567430986428) +\
                                       (oil_refined["AMMONIA_shapdis"] * 0.0829803298426259) +\
                                       (oil_refined["UREA_INDUS__shapdis"] * 0.701452239170947)
oil_refined["RAW_MATERIAL_shapdis"] = oil_refined["GAS_shapdis"] * 430 * 1.1

oil_refined["کرک شپدیس"] = oil_refined["FINAL_PRODUCT_shapdis"] - oil_refined["RAW_MATERIAL_shapdis"]

####################################################################################################

oil_refined = oil_refined.merge(dim_date, on="date", how="left")
oil_refined.drop_duplicates(subset=["date"], keep="first", inplace=True, ignore_index=True)

col_name_mappper_1 = {
    "gasoline_91_eu5": "بنزین 91 یورو 5", "gasoline_91_eu4": "بنزین 91 یورو 4", "gasoline_91_eu3": "بنزین 91 یورو 3",
    "gasoline_91_eu2": "بنزین 91 یورو 2", "gasoline_91_eu1": "بنزین 91 یورو 1", "gasoline_87_eu5": "بنزین 87 یورو 5",
    "gasoline_87_eu4": "بنزین 87 یورو 4", "gasoline_87_eu3": "بنزین 87 یورو 3", "gasoline_87_eu2": "بنزین 87 یورو 2",
    "gasoline_87_eu1": "بنزین 87 یورو 1"}
col_name_mappper_2 = {
    "hsfo_180_normal": "نفت کوره 180", "hsfo_380_normal": "نفت کوره 380", "hsfo_230_normal": "نفت کوره 230",
    "hsfo_280_normal": "نفت کوره 280", "hsfo_420_normal": "نفت کوره 420", "hsfo_720_normal": "نفت کوره 720",
    "hsfo_180_high_sulfur": "نفت کوره 180 گوگرد بالا", "hsfo_380_high_sulfur": "نفت کوره 380 گوگرد بالا",
    "hsfo_230_high_sulfur": "نفت کوره 230 گوگرد بالا", "hsfo_280_high_sulfur": "نفت کوره 280 گوگرد بالا",
    "hsfo_420_high_sulfur": "نفت کوره 420 گوگرد بالا", "hsfo_720_high_sulfur": "نفت کوره 720 گوگرد بالا"}
col_name_mappper_3 = {"gasoil_10": "نفت گاز تا 10 ppm", "gasoil_10_50": "نفت گاز بین 10 و 50 ppm",
                      "gasoil_50_500": "نفت گاز بین 50 و 500 ppm", "gasoil_500_2500": "نفت گاز بین 500 و 2500 ppm",
                      "gasoil_2500_5000": "نفت گاز بین 2500 و 5000 ppm", "gasoil_5000": "نفت گاز بیشتر از 5000 ppm"}

oil_refined.rename(columns=col_name_mappper_1, inplace=True)
oil_refined.rename(columns=col_name_mappper_2, inplace=True)
oil_refined.rename(columns=col_name_mappper_3, inplace=True)

crack_name = [
    "کرک شبندر قدیم", "کرک شبندر جدید",
    "کرک شپنا قدیم", "کرک شپنا جدید",
    "کرک شاوان قدیم", "کرک شاوان جدید",
    "کرک شتران قدیم", "کرک شتران جدید",
    "کرک نوری قدیم", "کرک نوری جدید",
    "کرک شجم", "کرک جم", "کرک شپدیس",
    "بنزین 91 یورو 5", "بنزین 91 یورو 4", "بنزین 91 یورو 3", "بنزین 91 یورو 2", "بنزین 91 یورو 1",
    "بنزین 87 یورو 5", "بنزین 87 یورو 4", "بنزین 87 یورو 3", "بنزین 87 یورو 2", "بنزین 87 یورو 1",
    "نفت کوره 180", "نفت کوره 230", "نفت کوره 280", "نفت کوره 380", "نفت کوره 420", "نفت کوره 720",
    "نفت کوره 180 گوگرد بالا", "نفت کوره 230 گوگرد بالا", "نفت کوره 280 گوگرد بالا",
    "نفت کوره 380 گوگرد بالا", "نفت کوره 420 گوگرد بالا", "نفت کوره 720 گوگرد بالا",
    "نفت گاز تا 10 ppm", "نفت گاز بین 10 و 50 ppm", "نفت گاز بین 50 و 500 ppm",
    "نفت گاز بین 500 و 2500 ppm", "نفت گاز بین 2500 و 5000 ppm", "نفت گاز بیشتر از 5000 ppm",
    "کرک شغدیر", "کرک شگویا"
]

chart_df = pd.DataFrame(columns=["date", "Miladi", "commodity", "price"])
for i in range(len(crack_name)):
    temp = oil_refined[["date", "Miladi", crack_name[i]]]
    temp["commodity"] = crack_name[i]
    temp.rename(mapper={crack_name[i]: "price"}, axis=1, inplace=True)
    temp.dropna(axis=0, subset="price", inplace=True, ignore_index=True)
    chart_df = pd.concat([chart_df, temp], axis=0, ignore_index=True)

chart_df.rename(mapper={"date": "date_jalali", "Miladi": "date"}, inplace=True, axis=1)

chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["owner"] = ["razmehgir"] * len(chart_df)
chart_df["unit"] = ["دلار بر تن"] * len(chart_df)
chart_df["name"] = chart_df["commodity"] + " - " + chart_df["reference"] + " (" + chart_df["unit"] + ")"

ll = chart_df["name"].unique().tolist()
for k in tqdm(range(len(ll))):
    nn = ll[k]
    temp_df = chart_df[chart_df["name"] == nn]
    temp_df.drop_duplicates(subset=["date"], keep="first", inplace=True, ignore_index=True)
    cursor = db_conn.cursor()
    cursor.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{nn}'")
    cursor.close()
    insert_to_database(dataframe=temp_df, database_table="[nooredenadb].[commodity].[commodities_data]", loading=False)

####################################################################################################

commodity_metal = pd.DataFrame(columns=["name", "commodity", "reference"], data=[
    ["slab", "اسلب", "متال بولتن"], ["hotroll_cis", "ورق گرم CIS", "متال بولتن"]])

metals_df = pd.DataFrame(columns=["date"])
for c in range(len(commodity_metal)):
    name, commodity, reference = commodity_metal.loc[c]
    temp = pd.read_sql(f"SELECT date_jalali AS date, price AS [{name}] FROM [nooredenadb].[commodity].[commodities_data]"
                       f" WHERE commodity='{commodity}' AND reference='{reference}'", db_conn)
    metals_df = metals_df.merge(temp, on="date", how="outer")
metals_df.sort_values(by="date", inplace=True, ascending=True, ignore_index=True)
metals_df.fillna(method="ffill", inplace=True)
metals_df["اسپرد ورق و اسلب"] = metals_df["hotroll_cis"] - metals_df["slab"]

metals_df = metals_df.merge(dim_date, on="date", how="left")

####################################################################################################

crack_metal_names = ["اسپرد ورق و اسلب"]

chart_df = pd.DataFrame(columns=["date", "Miladi", "commodity", "price"])
for i in range(len(crack_metal_names)):
    temp = metals_df[["date", "Miladi", crack_metal_names[i]]]
    temp["commodity"] = crack_metal_names[i]
    temp.rename(mapper={crack_metal_names[i]: "price"}, axis=1, inplace=True)
    temp.dropna(axis=0, subset="price", inplace=True, ignore_index=True)
    chart_df = pd.concat([chart_df, temp], axis=0, ignore_index=True)
chart_df.rename(mapper={"date": "date_jalali", "Miladi": "date"}, inplace=True, axis=1)
chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["owner"] = ["davoudian"] * len(chart_df)
chart_df["unit"] = ["دلار بر تن"] * len(chart_df)
chart_df["name"] = chart_df["commodity"] + " - " + chart_df["reference"] + " (" + chart_df["unit"] + ")"

ll = chart_df["name"].unique().tolist()
for k in tqdm(range(len(ll))):
    nn = ll[k]
    temp_df = chart_df[chart_df["name"] == nn]
    cursor = db_conn.cursor()
    cursor.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{nn}'")
    cursor.close()
    insert_to_database(dataframe=temp_df, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################
####################################################################################################

query_copper = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS copper FROM (SELECT date,"
                " TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-3-16' and "
                "Quantity > 0 AND (Symbol LIKE '%-CCAA-%' OR Symbol LIKE '%-OACCAA-%') union SELECT date, TotalPrice,"
                " Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-3-16' and Quantity > 0 AND ("
                "Symbol LIKE '%-CCAA-%' OR Symbol LIKE '%-OACCAA-%')) AS TEMP1 group by date ORDER BY date")
copper = pd.read_sql(query_copper, db_conn)

query_zinc = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS zinc FROM (SELECT date,"
              " TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-4-18' and "
              "Quantity > 0 union SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE"
              " Category='1-4-18' and Quantity > 0) AS TEMP1 group by date ORDER BY date")
zinc = pd.read_sql(query_zinc, db_conn)

query_aluminium = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS aluminium FROM (SELECT"
                   " date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-2-11'"
                   " and Quantity > 0 union SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_today]"
                   " WHERE Category='1-2-11' and Quantity > 0) AS TEMP1 group by date ORDER BY date")
aluminium = pd.read_sql(query_aluminium, db_conn)

query_aluminium_oxide = ("SELECT date, CAST(SUM(TotalPrice) AS float) / CAST(SUM(Quantity) AS float) AS aluminium_oxide"
                         " FROM (SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE"
                         " Category = '1-2-363' and Quantity > 0 UNION SELECT date, TotalPrice, Quantity FROM "
                         "[nooredenadb].[ime].[ime_data_today] WHERE Category = '1-2-363' and Quantity > 0) AS TEMP1"
                         " GROUP BY date ORDER BY date")
aluminium_oxide = pd.read_sql(query_aluminium_oxide, db_conn)

query_bb = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS bb FROM (SELECT date, "
            "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-1-1' and date >"
            " '1400/01/01' and ProducerName='فولاد خوزستان' and Quantity > 0 union SELECT date, TotalPrice, Quantity"
            " FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-1' and ProducerName='فولاد خوزستان' and"
            " Quantity > 0) AS TEMP1 group by date ORDER BY date")
bb = pd.read_sql(query_bb, db_conn)

query_concentrate = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS concentrate FROM ("
                     "SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE "
                     "Category='1-49-477' and date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, "
                     "Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-49-477' and Quantity > 0) "
                     "AS TEMP1 group by date ORDER BY date")
concentrate = pd.read_sql(query_concentrate, db_conn)

query_pellitized = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS pellitized FROM ("
                    "SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE "
                    "Category='1-49-464' and date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, "
                    "Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-49-464' and Quantity > 0) "
                    "AS TEMP1 group by date ORDER BY date")
pellitized = pd.read_sql(query_pellitized, db_conn)

query_dri = ("SELECT date, CAST(SUM(TotalPrice) AS float) / CAST(SUM(Quantity) AS float) AS dri FROM (SELECT date, "
             "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-97-452' and"
             " date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, Quantity FROM "
             "[nooredenadb].[ime].[ime_data_today] WHERE Category='1-97-452' and Quantity > 0) AS TEMP1"
             " group by date ORDER BY date")
dri = pd.read_sql(query_dri, db_conn)

query_rebar = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS rebar FROM (SELECT date, "
               "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category in "
               "('1-1-6', '1-1-7', '1-1-21', '1-1-64') and date > '1400/01/01' and Quantity > 0 union SELECT date, "
               "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in "
               "('1-1-6', '1-1-7', '1-1-21', '1-1-64') and Quantity > 0) AS TEMP1 group by date ORDER BY date")
rebar = pd.read_sql(query_rebar, db_conn)

query_rebar_alloy = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS rebar_alloy FROM ("
                     "SELECT date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE "
                     "Category='1-1-2682' and date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, "
                     "Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-2682' and Quantity > 0) "
                     "AS TEMP1 group by date ORDER BY date")
rebar_alloy = pd.read_sql(query_rebar_alloy, db_conn)

query_slab = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS slab FROM (SELECT date, "
              "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-1-152' and "
              "ProducerName='فولاد خوزستان' and date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, "
              "Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-152' and "
              "ProducerName='فولاد خوزستان' and Quantity > 0) AS TEMP1 group by date ORDER BY date")
slab = pd.read_sql(query_slab, db_conn)

query_hr = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS hr FROM (SELECT date, "
            "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category in ('1-1-9', "
            "'1-1-2520') and ProducerName='فولاد مبارکه اصفهان' and date > '1400/01/01' and Quantity > 0 union SELECT"
            " date, TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in ('1-1-9', "
            "'1-1-2520') and ProducerName='فولاد مبارکه اصفهان' and Quantity > 0) AS TEMP1 group by date ORDER BY date")
hr = pd.read_sql(query_hr, db_conn)

query_gr = ("SELECT date, CAST(SUM(TotalPrice) AS float)/CAST(SUM(Quantity) AS float) AS gr FROM (SELECT date, "
            "TotalPrice, Quantity FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category in ('1-1-17', "
            "'1-1-2521', '1-1-2522') and date > '1400/01/01' and Quantity > 0 union SELECT date, TotalPrice, Quantity"
            " FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in ('1-1-17', '1-1-2521', '1-1-2522') and"
            " Quantity > 0) AS TEMP1 group by date ORDER BY date")
gr = pd.read_sql(query_gr, db_conn)

dfs = [copper, zinc, aluminium, aluminium_oxide, bb, concentrate, pellitized, dri, rebar, rebar_alloy, slab, hr, gr]
dataframe = reduce(lambda left, right: pd.merge(left,right,on=['date'], how='outer'), dfs)
dataframe.sort_values(by="date", inplace=True, ignore_index=True)
dataframe.fillna(method="ffill", axis=0, inplace=True)
dataframe.fillna(method="bfill", axis=0, inplace=True)

dataframe["concentrate_bb_ratio"] = dataframe["concentrate"] / dataframe["bb"]
dataframe["concentrate_bb_spread"] = dataframe["bb"] - dataframe["concentrate"]
dataframe["pellitized_bb_ratio"] = dataframe["pellitized"] / dataframe["bb"]
dataframe["pellitized_bb_spread"] = dataframe["bb"] - dataframe["pellitized"]
dataframe["dri_bb_ratio"] = dataframe["dri"] / dataframe["bb"]
dataframe["dri_bb_spread"] = dataframe["bb"] - dataframe["dri"]
dataframe["rebar_bb_ratio"] = dataframe["rebar"] / dataframe["bb"]
dataframe["rebar_bb_spread"] = dataframe["rebar"] - dataframe["bb"]
dataframe["rebar_alloy_bb_ratio"] = dataframe["rebar_alloy"] / dataframe["bb"]
dataframe["rebar_alloy_bb_spread"] = dataframe["rebar_alloy"] - dataframe["bb"]
dataframe["hr_slab_ratio"] = dataframe["hr"] / dataframe["slab"]
dataframe["hr_slab_spread"] = dataframe["hr"] - dataframe["slab"]
dataframe["gr_hr_ratio"] = dataframe["gr"] / dataframe["hr"]
dataframe["gr_hr_spread"] = dataframe["gr"] - dataframe["hr"]
dataframe["aluminium_oxide_aluminium_ratio"] = dataframe["aluminium_oxide"] / dataframe["aluminium"]


mpr = {"copper": "مس کاتدی (بورس کالا)", "zinc": "شمش روی  (بورس کالا)", "aluminium": "شمش آلومینیوم (بورس کالا)",
       "aluminium_oxide": "اکسید آلومینیوم (بورس کالا)", "bb": "شمش (فولاد خوزستان)", "concentrate": "کنسانتره سنگ آهن",
       "pellitized": "گندله", "dri": "آهن اسفنجی", "rebar": "میلگرد", "rebar_alloy": "میلگرد فولاد آلیاژی",
       "slab": "اسلب (فولاد خوزستان)", "hr": "ورق گرم (فولاد مبارکه اصفهان)", "gr": "ورق گالوانیزه",
       "concentrate_bb_ratio": "نسبت کنسانتره به شمش", "concentrate_bb_spread": "اسپرد کنسانتره و شمش (ریال)",
       "pellitized_bb_ratio": "نسبت گندله به شمش", "pellitized_bb_spread": "اسپرد گندله و شمش (ریال)",
       "dri_bb_ratio": "نسبت آهن اسفنجی به شمش", "dri_bb_spread": "اسپرد آهن اسفنجی به شمش",
       "rebar_bb_ratio": "نسبت میلگرد به شمش", "rebar_bb_spread": "اسپرد میلگرد و شمش (ریال)",
       "rebar_alloy_bb_ratio": "نسبت میلگرد فولاد آلیاژی به شمش",
       "rebar_alloy_bb_spread": "اسپرد میلگرد فولاد آلیاژی و شمش (ریال)", "hr_slab_ratio": "نسبت ورق گرم به اسلب",
       "hr_slab_spread": "اسپرد ورق گرم و اسلب (ریال)", "gr_hr_ratio": "نسبت ورق گالوانیزه به ورق گرم",
       "gr_hr_spread": "اسپرد ورق گالوانیزه و ورق گرم (ریال)",
       "aluminium_oxide_aluminium_ratio": "نسبت آلومینا به آلومینیوم (بورس کالا)"}
dataframe.rename(mpr, axis=1, inplace=True)

cols = list(mpr.values())
chart_df = pd.melt(dataframe, id_vars=["date"], value_vars=cols, var_name="commodity", value_name="price")
chart_df["date"].replace("/", "-", regex=True, inplace=True)
chart_df = chart_df.merge(dim_date, on="date", how="left")
chart_df.rename({"Miladi": "date", "date": "date_jalali"}, axis=1, inplace=True)
chart_df["owner"] = ["davoudian"] * len(chart_df)
chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["name"] = chart_df["commodity"]
chart_df["unit"] = ["-"] * len(chart_df)

names = chart_df["name"].unique().tolist()
for n in tqdm(range(len(names))):
    cursor_ = db_conn.cursor()
    cursor_.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{names[n]}'")
    cursor_.close()

insert_to_database(dataframe=chart_df, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################

sugar = pd.read_sql("SELECT date, date_jalali, price as sugar FROM [nooredenadb].[commodity].[commodities_data] "
                    "where commodity='شکر تضمینی' AND unit='ریال بر تن'", db_conn)
wheat = pd.read_sql("SELECT date, date_jalali, price as wheat FROM [nooredenadb].[commodity].[commodities_data] "
                    "where commodity='گندم تضمینی'", db_conn)
beet = pd.read_sql("SELECT date, date_jalali, price as beet FROM [nooredenadb].[commodity].[commodities_data] "
                   "where commodity='چغندر تضمینی'", db_conn)
dollar = pd.read_sql("SELECT date, date_jalali, price as dollar FROM [nooredenadb].[commodity].[commodities_data] "
                     "WHERE name='دلار نیما - tgju.org (ریال بر دلار)'", db_conn)

sugar_dollar = dollar.merge(sugar, on=["date", "date_jalali"], how="outer").sort_values(
    by="date", inplace=False, ignore_index=True)
sugar_dollar["dollar"] = round(sugar_dollar["dollar"].interpolate(method='linear', limit_direction='forward', axis=0))
sugar_dollar["sugar"].ffill(inplace=True, axis=0)
sugar_dollar.dropna(subset=["dollar"], axis=0, inplace=True, ignore_index=True)
sugar_dollar["price"] = sugar_dollar["sugar"] / sugar_dollar["dollar"]

sugar_dollar = sugar_dollar[["date", "date_jalali", "price"]]
sugar_dollar[["owner", "commodity", "unit", "reference"]] = ["pirnajafi", "شکر تضمینی",
                                                             "دلار بر تن", "وزارت جهاد کشاورزی"]
sugar_dollar["name"] = sugar_dollar["commodity"] + " - " + sugar_dollar["reference"] + " (" + sugar_dollar["unit"] + ")"

crsr = db_conn.cursor()
crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='شکر تضمینی' AND unit='دلار بر تن'")
crsr.close()
insert_to_database(dataframe=sugar_dollar, database_table="[nooredenadb].[commodity].[commodities_data]")

# sugar_beet = sugar.merge(beet, on=["date", "date_jalali"], how="outer")
# sugar_beet["price"] = sugar_beet["sugar"] / sugar_beet["beet"]
# sugar_beet = sugar_beet[["date", "date_jalali", "price"]]
# sugar_beet[["owner", "commodity", "unit", "reference"]] = ["pirnajafi", "نسبت شکر به چغندر تضمینی", "-", "کارشناس"]
# sugar_beet["name"] = sugar_beet["commodity"]
# crsr = db_conn.cursor()
# crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='نسبت شکر به چغندر تضمینی'")
# crsr.close()
# insert_to_database(dataframe=sugar_beet, database_table="[nooredenadb].[commodity].[commodities_data]")

# beet_wheat = beet.merge(wheat, on=["date", "date_jalali"], how="outer")
# beet_wheat["price"] = beet_wheat["beet"] / beet_wheat["wheat"]
# beet_wheat = beet_wheat[["date", "date_jalali", "price"]]
# beet_wheat[["owner", "commodity", "unit", "reference"]] = ["pirnajafi", "نسبت چغندر به گندم تضمینی", "-", "کارشناس"]
# beet_wheat["name"] = beet_wheat["commodity"]
# crsr = db_conn.cursor()
# crsr.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE commodity='نسبت چغندر به گندم تضمینی'")
# crsr.close()
# insert_to_database(dataframe=beet_wheat, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################

commodities = ["کرک نوری جدید", "کرک شبندر جدید"]
table = pd.DataFrame()
for i in range(len(commodities)):
    df = pd.read_sql(f"SELECT [date], [price], [commodity] FROM [nooredenadb].[commodity].[commodities_data] where "
                     f"commodity='{commodities[i]}' ORDER BY [date]", db_conn)
    tmp = pd.DataFrame(data={
        "name": [df["commodity"].iloc[0]],
        "current_price": [df["price"].iloc[-1]],
        "current_change": [df["price"].iloc[-1] - df["price"].iloc[-2]],
        "current_change_percent": [round(100*(df["price"].iloc[-1] - df["price"].iloc[-2])/df["price"].iloc[-2],
                                         ndigits=5)]
    })
    table = pd.concat([table, tmp], axis=0, ignore_index=True)

for i in range(len(table)):
    name_ = table['name'].iloc[i]
    crsr = db_conn.cursor()
    crsr.execute(f"UPDATE [nooredenadb].[commodity].[commodity_data_today] SET price={table['current_price'].iloc[i]}, "
                 f"price_change={table['current_change'].iloc[i]}, "
                 f"change_percent={table['current_change_percent'].iloc[i]} WHERE name='{name_}'")
    crsr.close()
