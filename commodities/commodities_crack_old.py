import warnings
import jdatetime
import numpy as np
import pandas as pd
from tqdm import tqdm

from utils.database import make_connection, insert_to_database


warnings.filterwarnings("ignore")
result_path = "D:/database/commodity/results.xlsx"
today = jdatetime.datetime.today()
powerbi_database = make_connection()

cmdty = pd.read_sql("SELECT [date], [date_jalali], [price] ,[commodity] ,[reference] FROM "
                    "[nooredenadb].[commodity].[commodities_data]", powerbi_database)

brent = cmdty[cmdty["commodity"] == "نفت برنت"]
dubai = cmdty[cmdty["commodity"] == "نفت دبی"]
oman = cmdty[cmdty["commodity"] == "نفت عمان"]
brent_days = brent["date_jalali"].unique().tolist()
dubai_days = dubai["date_jalali"].unique().tolist()
oman_days = oman["date_jalali"].unique().tolist()

missing_days = []
for i in range(len(brent_days)):
    if brent_days[i] not in oman_days:
        missing_days.append(brent_days[i])
days = brent_days + dubai_days + oman_days
days = list(set(days))

oil = pd.DataFrame(data={"date": days, "brent": [np.nan]*len(days),
                         "dubai": [np.nan]*len(days), "oman": [np.nan]*len(days)})
for i in range(len(oil)):
    date = oil["date"].iloc[i]
    brent_temp = brent[brent["date_jalali"] == date]
    dubai_temp = dubai[dubai["date_jalali"] == date]
    oman_temp = oman[oman["date_jalali"] == date]
    if len(brent_temp) != 0:
        brent_temp.reset_index(drop=True, inplace=True)
        oil["brent"].iloc[i] = brent_temp["price"].iloc[0]
    if len(dubai_temp) != 0:
        dubai_temp.reset_index(drop=True, inplace=True)
        oil["dubai"].iloc[i] = dubai_temp["price"].iloc[0]
    if len(oman_temp) != 0:
        oman_temp.reset_index(drop=True, inplace=True)
        oil["oman"].iloc[i] = oman_temp["price"].iloc[0]

oil.sort_values("date", inplace=True, ignore_index=True)
oil.fillna(method="ffill", axis=0, inplace=True)

oil["light_oil"] = (((oil["brent"] + oil["dubai"] + oil["oman"])/3)-5) * 0.95
oil["heavy_oil"] = (((oil["brent"] + oil["dubai"] + oil["oman"])/3)-6) * 0.95
commodity_petrol = pd.DataFrame(columns=["name", "commodity", "reference"], data=[
    ["gasoil_0.05_pg", "نفت گاز _ 0.05", "خلیج فارس پلتس"], ["gasoil_0.25_pg", "نفت گاز _ 0.25", "خلیج فارس پلتس"],
    ["gasoil_0.005_pg", "نفت گاز _ 0.005", "خلیج فارس پلتس"], ["gasoline_95_sng", "بنزین_95", "سنگاپور پلتس"],
    ["gasoline_92_sng", "بنزین_92", "سنگاپور پلتس"], ["hsfo_380_pg", "نفت کوره ـ 380", "خلیج فارس پلتس"],
    ["vb_nrc", "وکیوم باتوم", "شرکت ملی پالایش نرخ دلاری"], ["kerosene_pg", "نفت سفید", "خلیج فارس پلتس"],
    ["lubecut_heavy_nrc", "لوبکات سنگین", "شرکت ملی پالایش نرخ دلاری"], ["lpg_sng", "LPG پلتس", "فوب سنگاپور"],
    ["naphta_pg", "نفتا", "خلیج فارس پلتس"], ["gasoil_0.001_pg", "10ppm _ نفت گاز", "خلیج فارس پلتس"],
    ["gasoline_95_pg", "بنزین_95", "خلیج فارس پلتس"], ["gasoline_92_pg", "بنزین_92", "خلیج فارس پلتس"],
    ["condensate_pj", "میعانات گازی", "پارس جنوبی پلتس"], ["naphta_lr2_pg", "نفتا LR2", "خلیج فارس پلتس"],
    ["benzene_ch", "بنزن پلتس", "چین پلتس"], ["benzene_pg", "بنزن داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["px_se", "پارازایلین پلتس", "جنوب شرق آسیا پلتس"], ["px_pg", "پارازایلین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["pbr1202", "PBR1202", "دفتر توسعه پتروشیمی نرخ دلاری"], ["pbr1220", "PBR1220", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["ox_ch", "آرتوزایلین پلتس", "چین پلتس"], ["ox_pg", "آرتوزایلین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["sbr1712", "استایرن بوتادین رابر 1712", "دفتر توسعه پتروشیمی نرخ دلاری"], ["styren", "استایرن پلتس", "چین پلتس"],
    ["sbr1502", "استایرن بوتادین رابر روشن 1502", "دفتر توسعه پتروشیمی نرخ دلاری"], ["urea_irfob", "اوره", "فوب ایران"],
    ["butadin", "بوتادین داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"], ["butane_pg", "بوتان پلتس", "فوب خلیج فارس"],
    ["polyethylene_light_me", "پلی اتیلن سبک پلتس", "خاورمیانه"], ["hsfo_180_pg", "نفت کوره ـ 180", "خلیج فارس پلتس"],
    ["polyethylene_heavy_me", "پلی اتیلن سنگین پلتس", "خاورمیانه"], ["ethylene_se", "اتیلن پلتس", "جنوب شرق آسیا پلتس"],
    ["propylene_se", "پروپیلن پلتس", "جنوب شرق آسیا پلتس"], ["propane_pdid", "پروپان", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["butene_one_pdid", "بوتن وان داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"], ["urea_brzilcfr", "اوره", "سی اف آر برزیل"],
    ["gasoline_pyrolysis", "بنزین پیرولیز داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"], ["gas", "گاز خوراک", "وزارت نفت"],
    ["panthan_plus_pdid", "نفتا(پنتان پلاس)", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["c3_pdid", "سی تری پلاس داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["lg_pdid", "گاز مایع", "دفتر توسعه پتروشیمی نرخ دلاری"],
    ["ammonia_pdid", "آمونیاک داخلی", "دفتر توسعه پتروشیمی نرخ دلاری"]
])

days_commodity_petrol = []
for i in range(len(commodity_petrol)):
    days_commodity_petrol += list(cmdty[(cmdty["commodity"] == commodity_petrol["commodity"].iloc[i]) & (
            cmdty["reference"] == commodity_petrol["reference"].iloc[i])]["date_jalali"].unique())
days_commodity_petrol = list(set(days_commodity_petrol))
oil_refined = pd.DataFrame()
oil_refined["date"] = days_commodity_petrol
for i in range(len(commodity_petrol)):
    temp = cmdty[(cmdty["commodity"] == commodity_petrol["commodity"].iloc[i]) & (
            cmdty["reference"] == commodity_petrol["reference"].iloc[i])]
    temp = temp[["date_jalali", "price"]]
    temp.rename({"date_jalali": "date", "price": f"{commodity_petrol['name'].iloc[i]}"}, axis=1, inplace=True)
    oil_refined = oil_refined.merge(temp, how="outer", on="date")
oil_refined.sort_values(by="date", inplace=True, ascending=True, ignore_index=True)

for i in range(len(oil_refined)):
    if oil_refined["date"].iloc[i] <= "1401-05-01":
        oil_refined["naphta_lr2_pg"].iloc[i] = oil_refined["naphta_pg"].iloc[i] * 1.0164162998

result_tmp = pd.read_excel(result_path, sheet_name="APAG Data")
result_tmp.replace("None", np.nan, inplace=True)
result_tmp = result_tmp[result_tmp["Unnamed: 0"] == "Gasoil 10 ppm(FOB Persian Gulf)($/barrel)"]
result_tmp.reset_index(drop=True, inplace=True)
result_tmp = result_tmp.T.iloc[1:, :]
result_tmp.reset_index(drop=False, inplace=True, names="date")
result_tmp.rename(mapper={0: "price"}, axis=1, inplace=True)
result_tmp["date_jalali"] = [jdatetime.datetime.fromgregorian(
    year=int(result_tmp["date"].iloc[i][:4]), month=int(result_tmp["date"].iloc[i][5:7]),
    day=int(result_tmp["date"].iloc[i][8:])).strftime(format="%Y-%m-%d") for i in range(len(result_tmp))]

for i in range(len(oil_refined)):
    if (oil_refined["date"].iloc[i] in result_tmp["date_jalali"].values) and \
            (oil_refined["gasoil_0.001_pg"].iloc[i] != oil_refined["gasoil_0.001_pg"].iloc[i]):
        oil_refined["gasoil_0.001_pg"].iloc[i] = result_tmp["price"].iloc[
            result_tmp.index[result_tmp["date_jalali"] == oil_refined["date"].iloc[i]].values[0]]

oil_refined["vb_nrc"].fillna(method="ffill", inplace=True)
oil_refined["lubecut_heavy_nrc"].fillna(method="ffill", inplace=True)
oil_refined["pbr1220"].fillna(method="ffill", inplace=True)
oil_refined["pbr1202"].fillna(method="ffill", inplace=True)
oil_refined["sbr1712"].fillna(method="ffill", inplace=True)
oil_refined["sbr1502"].fillna(method="ffill", inplace=True)
oil_refined["styren"].fillna(method="ffill", inplace=True)
oil_refined["butadin"].fillna(method="ffill", inplace=True)
oil_refined["butene_one_pdid"].fillna(method="ffill", inplace=True)
oil_refined["lg_pdid"].fillna(method="ffill", inplace=True)
oil_refined["panthan_plus_pdid"].fillna(method="ffill", inplace=True)
oil_refined["c3_pdid"].fillna(method="ffill", inplace=True)
oil_refined = oil_refined[oil_refined["date"] > "1400-09-10"]
oil_refined.reset_index(drop=True, inplace=True)
# oil_refined.dropna(subset="gasoil_0.05_pg", axis=0, inplace=True, ignore_index=True)

lpg_missing_data = cmdty[cmdty["commodity"] == "گاز مایع"]
lpg_missing_data.reset_index(drop=True, inplace=True)
for i in range(len(oil_refined)):
    if oil_refined["date"].iloc[i] < "1401-05-03":
        temp = lpg_missing_data[lpg_missing_data["date_jalali"] <= oil_refined["date"].iloc[i]]
        temp.sort_values(by="date_jalali", ascending=True, inplace=True, ignore_index=True)
        oil_refined["lpg_sng"].iloc[i] = temp["price"].iloc[-1]
oil_refined.fillna(method="ffill", inplace=True)
oil_refined.fillna(method="bfill", inplace=True)

########################################################################################################################

oil_refined["alpha"] = oil_refined["gasoil_0.25_pg"] / oil_refined["gasoil_0.05_pg"]

########################################################################################################################

oil_refined["GASOIL_bandar"] = (0.63 * oil_refined["gasoil_0.005_pg"]) + (
        0.37 * 0.95 * oil_refined["alpha"] * oil_refined["gasoil_0.25_pg"])
oil_refined["GASOLINE_bandar"] = (0.64 * (oil_refined["gasoline_95_sng"] - (5 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])))+ (
        0.36 * (oil_refined["gasoline_95_sng"] - (2 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])))

oil_refined["GASOLINE_bandar_new"] = ((0.64 * (oil_refined["gasoline_95_pg"] - (12 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])))+
                                      (0.36 * (oil_refined["gasoline_95_pg"] - (4 / 3) * (
                                              oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"]))))

oil_refined["HSFO"] = oil_refined["hsfo_380_pg"] * (0.963 * 158.9873 / 1000)
oil_refined["VB"] = oil_refined["vb_nrc"] * (1.007 * 158.9873 / 1000)
oil_refined["KEROSENE"] = oil_refined["kerosene_pg"] + 1
oil_refined["HEAVYLUBECUT"] = oil_refined["lubecut_heavy_nrc"] * (0.938 * 158.9873 / 1000)
oil_refined["HEAVYJETFUEL"] = oil_refined["kerosene_pg"] - 1
oil_refined["LPG"] = oil_refined["lpg_sng"] * (0.564 * 158.9873 / 1000)
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
oil_refined["REFINED_OIL_bandar_new"] = (oil_refined["GASOIL_bandar"] * 0.341441590160615) +\
                                    (oil_refined["GASOLINE_bandar_new"] * 0.237078210870903) +\
                                    (oil_refined["HSFO"] * 0.237053507799077) +\
                                    (oil_refined["VB"] * 0.0900039309230667) +\
                                    (oil_refined["KEROSENE"] * 0.0311227722742082) +\
                                    (oil_refined["HEAVYLUBECUT"] * 0.0236845686416851) +\
                                    (oil_refined["HEAVYJETFUEL"] * 0.0107937459227936) +\
                                    (oil_refined["LPG"] * 0.0176201780090371) +\
                                    (oil_refined["NAPHTA"] * 0.00677655452100248)

########################################################################################################################

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
oil_refined["REFINED_OIL_shpna_new"] = (oil_refined["GASOIL_shpna"] * 0.420016747750079) +\
                                    (oil_refined["GASOLINE_shpna_new"] * 0.214414536129383) +\
                                    (oil_refined["HSFO"] * 0.117926155859682) +\
                                    (oil_refined["VB"] * 0.111513104049267) +\
                                    (oil_refined["KEROSENE"] * 0.0100810603485273) +\
                                    (oil_refined["ISORECYCLE"] * 0.0110861438496628) +\
                                    (oil_refined["RAWOIL"] * 0.0310253982051612) +\
                                    (oil_refined["HEAVYJETFUEL"] * 0.0104068020846743) +\
                                    (oil_refined["LPG"] * 0.02438620260699) +\
                                    (oil_refined["NAPHTA"] * 0.0124706827885459)

########################################################################################################################

oil_refined["GASOIL_shavan"] = oil_refined["gasoil_0.005_pg"]

oil_refined["GASOLINE_shavan"] = oil_refined["gasoline_95_sng"] - (5 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])
oil_refined["GASOLINE_shavan_new"] = oil_refined["gasoline_95_pg"] - (9 / 3) * (
        oil_refined["gasoline_95_sng"] - oil_refined["gasoline_92_sng"])

oil_refined["NAPHTA_shavan"] = oil_refined["naphta_lr2_pg"] * 0.72 * 158.9873 / 1000
oil_refined["condensate_shavan"] = (oil_refined["condensate_pj"]-2)*0.95
oil_refined["REFINED_OIL_shavan"] = (oil_refined["HSFO"] * 0.208651351) +\
                                    (oil_refined["gasoil_0.005_pg"] * 0.296821798) +\
                                    (oil_refined["GASOLINE_shavan"] * 0.349211287) +\
                                    (oil_refined["NAPHTA_shavan"] * 0.133005252)
oil_refined["REFINED_OIL_shavan_new"] = (oil_refined["HSFO"] * 0.208651351) +\
                                    (oil_refined["gasoil_0.005_pg"] * 0.296821798) +\
                                    (oil_refined["GASOLINE_shavan_new"] * 0.349211287) +\
                                    (oil_refined["NAPHTA_shavan"] * 0.133005252)

########################################################################################################################

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

########################################################################################################################

oil_refined["light_oil"] = [np.nan] * len(oil_refined)
oil_refined["heavy_oil"] = [np.nan] * len(oil_refined)

for i in range(len(oil_refined)):
    if oil_refined["date"].iloc[i] in oil["date"].values:
        date = oil_refined["date"].iloc[i]
        idx = oil.index[oil["date"] == oil_refined["date"].iloc[i]].values[0]
        oil_refined["light_oil"].iloc[i] = oil["light_oil"].iloc[idx]
        oil_refined["heavy_oil"].iloc[i] = oil["heavy_oil"].iloc[idx]

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

################################################################################################################

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

################################################################################################################

oil_refined["PBR_shjam"] = (((oil_refined["pbr1202"] + oil_refined["pbr1220"]) / 2) * 0.92 * 0.43) + (
        ((oil_refined["pbr1202"] + oil_refined["pbr1220"]) / 2) * 0.92 * 0.96 * 0.57)
oil_refined["SBR_shjam"] = (((oil_refined["sbr1712"] + oil_refined["sbr1502"]) / 2) * 0.95 * 0.91) + (
        ((oil_refined["sbr1712"] + oil_refined["sbr1502"]) / 2) * 0.95 * 0.96 * 0.09)
oil_refined["FINAL_PRODUCT_shjam"] = (oil_refined["PBR_shjam"] * 0.562) + (oil_refined["SBR_shjam"] * 0.438)
oil_refined["RAW_MATERIAL_shjam"] = ((oil_refined["butadin"] * 0.79) + (oil_refined["styren"] * 0.18 * 0.44))/0.92
oil_refined["کرک شجم"] = oil_refined["FINAL_PRODUCT_shjam"] - oil_refined["RAW_MATERIAL_shjam"]

################################################################################################################

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
oil_refined["ETHANE_jam"] = [400 if oil_refined["ETHANE_jam"].iloc[i] >= 400 else
                             220 if oil_refined["ETHANE_jam"].iloc[i] <= 220 else
                             oil_refined["ETHANE_jam"].iloc[i]
                             for i in range(len(oil_refined))]
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

################################################################################################################

oil_refined["UREA_AGRIC__shapdis"] = oil_refined["urea_irfob"] * 0.75
oil_refined["AMMONIA_shapdis"] = oil_refined["ammonia_pdid"]
oil_refined["UREA_INDUS__shapdis"] = (oil_refined["urea_brzilcfr"] - 35) * 0.9
oil_refined["GAS_shapdis"] = [0.2 if
                              (oil_refined["date"].iloc[i] >= "1400-09-01") &
                              (oil_refined["date"].iloc[i] <="1401-12-30") &
                              (oil_refined["gas"].iloc[i] > 0.2)
                              else oil_refined["gas"].iloc[i] for i in range(len(oil_refined))]

oil_refined["FINAL_PRODUCT_shapdis"] = (oil_refined["UREA_AGRIC__shapdis"] * 0.215567430986428) +\
                                       (oil_refined["AMMONIA_shapdis"] * 0.0829803298426259) +\
                                       (oil_refined["UREA_INDUS__shapdis"] * 0.701452239170947)
oil_refined["RAW_MATERIAL_shapdis"] = oil_refined["GAS_shapdis"] * 430 * 1.1

oil_refined["کرک شپدیس"] = oil_refined["FINAL_PRODUCT_shapdis"] - oil_refined["RAW_MATERIAL_shapdis"]

################################################################################################################

oil_refined["date_g"] = [jdatetime.datetime.togregorian(jdatetime.datetime.strptime(
    oil_refined["date"].iloc[i], "%Y-%m-%d")).strftime("%Y-%m-%d") for i in range(len(oil_refined))]

oil_refined_names = oil_refined.columns.tolist()
for i in ['date_g', 'date']:
    if i in oil_refined_names:
        oil_refined_names.remove(i)

chart_df = pd.DataFrame(columns=["date", "date_g", "commodity", "price"])
for i in range(len(oil_refined_names)):
    temp = oil_refined[["date", "date_g", oil_refined_names[i]]]
    temp["commodity"] = [oil_refined_names[i]] * len(temp)
    temp.rename(mapper={oil_refined_names[i]: "price"}, axis=1, inplace=True)
    temp.dropna(axis=0, subset="price", inplace=True, ignore_index=True)
    chart_df = pd.concat([chart_df, temp], axis=0, ignore_index=True)

chart_df.rename(mapper={"date": "date_jalali", "date_g": "date"}, inplace=True, axis=1)
chart_df = chart_df[chart_df["commodity"].isin(["کرک شبندر قدیم", "کرک شبندر جدید","کرک شپنا قدیم", "کرک شپنا جدید",
                                                "کرک شاوان قدیم", "کرک شاوان جدید", "کرک شتران قدیم", "کرک شتران جدید",
                                                "کرک نوری قدیم", "کرک نوری جدید", "کرک شجم", "کرک جم", "کرک شپدیس"])]
chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["owner"] = ["razmehgir"] * len(chart_df)
chart_df["unit"] = ["دلار بر تن"] * len(chart_df)
chart_df["name"] = chart_df["commodity"] + " - " + chart_df["reference"] + " (" + chart_df["unit"] + ")"

ll = chart_df["name"].unique().tolist()
for k in tqdm(range(len(ll))):
    nn = ll[k]
    temp_df = chart_df[chart_df["name"] == nn]
    cursor = powerbi_database.cursor()
    cursor.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{nn}'")
    cursor.close()
    insert_to_database(dataframe=temp_df, database_table="[nooredenadb].[commodity].[commodities_data]")

########################################################################################################################

commodity_metal = pd.DataFrame(columns=["name", "commodity", "reference"], data=[
    ["slab", "اسلب", "متال بولتن"], ["hotroll_cis", "ورق گرم CIS", "متال بولتن"]])

days_commodity_metal = []
for i in range(len(commodity_metal)):
    days_commodity_metal += list(cmdty[(cmdty["commodity"] == commodity_metal["commodity"].iloc[i]) & (
            cmdty["reference"] == commodity_metal["reference"].iloc[i])]["date_jalali"].unique())
days_commodity_metal = list(set(days_commodity_metal))

metals_df = pd.DataFrame()
metals_df["date"] = days_commodity_metal

for i in range(len(commodity_metal)):
    metals_df[commodity_metal["name"].iloc[i]] = [np.nan] * len(metals_df)
    temp = cmdty[(cmdty["commodity"] == commodity_metal["commodity"].iloc[i]) & (
            cmdty["reference"] == commodity_metal["reference"].iloc[i])]
    temp.reset_index(drop=True, inplace=True)
    for j in range(len(metals_df)):
        date = metals_df["date"].iloc[j]
        temp_ = temp[temp["date_jalali"] == date]
        if len(temp_) != 0:
            temp_.reset_index(drop=True, inplace=True)
            metals_df[commodity_metal["name"].iloc[i]].iloc[j] = temp_["price"].iloc[0]

metals_df.sort_values(by="date", inplace=True, ascending=True, ignore_index=True)
metals_df.fillna(method="ffill", inplace=True)

metals_df["اسپرد ورق و اسلب"] = metals_df["hotroll_cis"] - metals_df["slab"]
metals_df["date_g"] = [jdatetime.datetime.togregorian(jdatetime.datetime.strptime(
    metals_df["date"].iloc[i], "%Y-%m-%d")).strftime(format="%Y-%m-%d") for i in range(len(metals_df))]

########################################################################################################################

metals_df_names = metals_df.columns.tolist()
for i in ['date_g', 'date']:
    if i in metals_df_names:
        metals_df_names.remove(i)

chart_df = pd.DataFrame(columns=["date", "date_g", "commodity", "price"])
for i in range(len(metals_df_names)):
    temp = metals_df[["date", "date_g", metals_df_names[i]]]
    temp["commodity"] = [metals_df_names[i]] * len(temp)
    temp.rename(mapper={metals_df_names[i]: "price"}, axis=1, inplace=True)
    temp.dropna(axis=0, subset="price", inplace=True, ignore_index=True)
    chart_df = pd.concat([chart_df, temp], axis=0, ignore_index=True)

chart_df.rename(mapper={"date": "date_jalali", "date_g": "date"}, inplace=True, axis=1)
chart_df = chart_df[chart_df["commodity"].isin(["اسپرد ورق و اسلب"])]
chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["owner"] = ["davoudian"] * len(chart_df)
chart_df["unit"] = ["دلار بر تن"] * len(chart_df)
chart_df["name"] = chart_df["commodity"] + " - " + chart_df["reference"] + " (" + chart_df["unit"] + ")"

ll = chart_df["name"].unique().tolist()
for k in tqdm(range(len(ll))):
    nn = ll[k]
    temp_df = chart_df[chart_df["name"] == nn]
    cursor = powerbi_database.cursor()
    cursor.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{nn}'")
    cursor.close()
    insert_to_database(dataframe=temp_df, database_table="[nooredenadb].[commodity].[commodities_data]")

########################################################################################################################
########################################################################################################################

copper = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-3-16' "
                     "and Quantity > 0 order by date", powerbi_database)
copper_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-3-16'"
                      " and Quantity > 0", powerbi_database)
if len(copper_) > 0:
    copper = pd.concat([copper, copper_], axis=0, ignore_index=True)
copper[["Symbol1", "Symbol2", "Symbol3", "Symbol4"]] = copper["Symbol"].str.split("-", expand=True)
copper = copper[copper["Symbol2"].isin(["CCAA", "OACCAA"])]
copper = copper.groupby("date", as_index=False).sum(numeric_only=True)
copper["copper"] = copper["TotalPrice"]/copper["Quantity"]
copper = copper[["date", "copper"]]

zinc = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-4-18'"
                   " and Quantity > 0 order by date", powerbi_database)
zinc_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-4-18'"
                    " and Quantity > 0", powerbi_database)
if len(zinc_) > 0:
    zinc = pd.concat([zinc, zinc_], axis=0, ignore_index=True)
zinc = zinc.groupby("date", as_index=False).sum(numeric_only=True)
zinc["zinc"] = zinc["TotalPrice"]/zinc["Quantity"]
zinc = zinc[["date", "zinc"]]

aluminium = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE Category='1-2-11'"
                        " and Quantity > 0 order by date", powerbi_database)
aluminium_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-2-11'"
                         " and Quantity > 0", powerbi_database)
if len(aluminium_) > 0:
    aluminium = pd.concat([aluminium, aluminium_], axis=0, ignore_index=True)
aluminium = aluminium.groupby("date", as_index=False).sum(numeric_only=True)
aluminium["aluminium"] = aluminium["TotalPrice"]/aluminium["Quantity"]
aluminium = aluminium[["date", "aluminium"]]

bb = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                 "Category='1-1-1' and ProducerName='فولاد خوزستان' and Quantity > 0 order by date", powerbi_database)
bb_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-1' and "
                  "ProducerName='فولاد خوزستان' and Quantity > 0 order by date", powerbi_database)
if len(bb_) > 0:
    bb = pd.concat([bb, bb_], axis=0, ignore_index=True)
bb = bb.groupby("date", as_index=False).sum(numeric_only=True)
bb["bb"] = bb["TotalPrice"]/bb["Quantity"]
bb = bb[["date", "bb"]]

concentrate = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                          "Category='1-49-477' and Quantity > 0 order by date", powerbi_database)
concentrate_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-49-477' and "
                           "Quantity > 0 order by date", powerbi_database)
if len(concentrate_) > 0:
    concentrate = pd.concat([concentrate, concentrate_], axis=0, ignore_index=True)
concentrate = concentrate.groupby("date", as_index=False).sum(numeric_only=True)
concentrate["concentrate"] = concentrate["TotalPrice"] / concentrate["Quantity"]
concentrate = concentrate[["date", "concentrate"]]

pellitized = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                         "Category='1-49-464' and Quantity > 0 order by date", powerbi_database)
pellitized_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-49-464' and "
                          "Quantity > 0 order by date", powerbi_database)
if len(pellitized_) > 0:
    pellitized = pd.concat([pellitized, pellitized_], axis=0, ignore_index=True)
pellitized = pellitized.groupby("date", as_index=False).sum(numeric_only=True)
pellitized["pellitized"] = pellitized["TotalPrice"] / pellitized["Quantity"]
pellitized = pellitized[["date", "pellitized"]]

dri = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                  "Category='1-97-452' and Quantity > 0 order by date", powerbi_database)
dri_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-97-452' and "
                   "Quantity > 0 order by date", powerbi_database)
if len(dri_) > 0:
    dri = pd.concat([dri, dri_], axis=0, ignore_index=True)
dri = dri.groupby("date", as_index=False).sum(numeric_only=True)
dri["dri"] = dri["TotalPrice"] / dri["Quantity"]
dri = dri[["date", "dri"]]

rebar = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and Category "
                    "in ('1-1-6', '1-1-7', '1-1-21', '1-1-64') and Quantity > 0 order by date", powerbi_database)
rebar_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in "
                     "('1-1-6', '1-1-7', '1-1-21', '1-1-64') and Quantity > 0 order by date", powerbi_database)
if len(rebar_) > 0:
    rebar = pd.concat([rebar, rebar_], axis=0, ignore_index=True)
rebar = rebar.groupby("date", as_index=False).sum(numeric_only=True)
rebar["rebar"] = rebar["TotalPrice"] / rebar["Quantity"]
rebar = rebar[["date", "rebar"]]

rebar_alloy = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                          "Category='1-1-2682' and Quantity > 0 order by date", powerbi_database)
rebar_alloy_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-2682' and "
                           "Quantity > 0 order by date", powerbi_database)
if len(rebar_alloy_) > 0:
    rebar_alloy = pd.concat([rebar_alloy, rebar_alloy_], axis=0, ignore_index=True)
rebar_alloy = rebar_alloy.groupby("date", as_index=False).sum(numeric_only=True)
rebar_alloy["rebar_alloy"] = rebar_alloy["TotalPrice"] / rebar_alloy["Quantity"]
rebar_alloy = rebar_alloy[["date", "rebar_alloy"]]

slab = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and "
                   "Category='1-1-152' and ProducerName='فولاد خوزستان' and Quantity > 0 order by date",
                   powerbi_database)
slab_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category='1-1-152' and "
                    "ProducerName='فولاد خوزستان' and Quantity > 0 order by date", powerbi_database)
if len(slab_) > 0:
    slab = pd.concat([slab, slab_], axis=0, ignore_index=True)
slab = slab.groupby("date", as_index=False).sum(numeric_only=True)
slab["slab"] = slab["TotalPrice"] / slab["Quantity"]
slab = slab[["date", "slab"]]

hr = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and Category in "
                 "('1-1-9', '1-1-2520') and ProducerName='فولاد مبارکه اصفهان' and Quantity > 0 order by date",
                 powerbi_database)
hr_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in ('1-1-9', '1-1-2520') and "
                  "ProducerName='فولاد مبارکه اصفهان' and Quantity > 0 order by date", powerbi_database)
if len(hr_) > 0:
    hr = pd.concat([hr, hr_], axis=0, ignore_index=True)
hr = hr.groupby("date", as_index=False).sum(numeric_only=True)
hr["hr"] = hr["TotalPrice"] / hr["Quantity"]
hr = hr[["date", "hr"]]

gr = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_historical] WHERE date > '1400/01/01' and Category in "
                 "('1-1-17', '1-1-2521', '1-1-2522') and Quantity > 0 order by date", powerbi_database)
gr_ = pd.read_sql("SELECT * FROM [nooredenadb].[ime].[ime_data_today] WHERE Category in "
                  "('1-1-17', '1-1-2521', '1-1-2522') and Quantity > 0 order by date", powerbi_database)
if len(gr_) > 0:
    gr = pd.concat([gr, gr_], axis=0, ignore_index=True)
gr = gr.groupby("date", as_index=False).sum(numeric_only=True)
gr["gr"] = gr["TotalPrice"] / gr["Quantity"]
gr = gr[["date", "gr"]]


copper.set_index("date", inplace=True)
zinc.set_index("date", inplace=True)
aluminium.set_index("date", inplace=True)
bb.set_index("date", inplace=True)
concentrate.set_index("date", inplace=True)
pellitized.set_index("date", inplace=True)
dri.set_index("date", inplace=True)
rebar.set_index("date", inplace=True)
rebar_alloy.set_index("date", inplace=True)
slab.set_index("date", inplace=True)
hr.set_index("date", inplace=True)
gr.set_index("date", inplace=True)

dataframe = pd.concat([copper, zinc, aluminium, bb, concentrate, pellitized, dri,
                       rebar, rebar_alloy, slab, hr, gr], axis=1, join="outer")
dataframe.sort_index(inplace=True)
dataframe.fillna(method="ffill", axis=0, inplace=True)
dataframe.fillna(method="bfill", axis=0, inplace=True)
dataframe.reset_index(drop=False, names="date", inplace=True)

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

cols = dataframe.columns.tolist()
cols.remove("date")
chart_df = pd.DataFrame()
for c in range(len(cols)):
    tmp = dataframe[["date", cols[c]]]
    tmp["commodity"] = [cols[c]] * len(tmp)
    tmp.rename(mapper={cols[c]: "price", "date": "date_jalali"}, axis=1, inplace=True)
    chart_df = pd.concat([chart_df, tmp], axis=0, ignore_index=True)

mpr = {"copper": "مس کاتدی (بورس کالا)", "zinc": "شمش روی  (بورس کالا)", "aluminium": "شمش آلومینیوم (بورس کالا)",
       "bb": "شمش (فولاد خوزستان)", "concentrate": "کنسانتره سنگ آهن", "pellitized": "گندله", "dri": "آهن اسفنجی",
       "rebar": "میلگرد", "rebar_alloy": "میلگرد فولاد آلیاژی", "slab": "اسلب (فولاد خوزستان)",
       "hr": "ورق گرم (فولاد مبارکه اصفهان)", "gr": "ورق گالوانیزه", "concentrate_bb_ratio": "نسبت کنسانتره به شمش",
       "concentrate_bb_spread": "اسپرد کنسانتره و شمش (ریال)", "pellitized_bb_ratio": "نسبت گندله به شمش",
       "pellitized_bb_spread": "اسپرد گندله و شمش (ریال)", "dri_bb_ratio": "نسبت آهن اسفنجی به شمش",
       "dri_bb_spread": "اسپرد آهن اسفنجی به شمش", "rebar_bb_ratio": "نسبت میلگرد به شمش",
       "rebar_bb_spread": "اسپرد میلگرد و شمش (ریال)", "rebar_alloy_bb_ratio": "نسبت میلگرد فولاد آلیاژی به شمش",
       "rebar_alloy_bb_spread": "اسپرد میلگرد فولاد آلیاژی و شمش (ریال)", "hr_slab_ratio": "نسبت ورق گرم به اسلب",
       "hr_slab_spread": "اسپرد ورق گرم و اسلب (ریال)", "gr_hr_ratio": "نسبت ورق گالوانیزه به ورق گرم",
       "gr_hr_spread": "اسپرد ورق گالوانیزه و ورق گرم (ریال)"}
chart_df.replace(mpr, inplace=True)

chart_df["date_jalali"].replace("/", "-", regex=True, inplace=True)

chart_df["date"] = [
    jdatetime.datetime.strptime(chart_df["date_jalali"].iloc[i], "%Y-%m-%d").togregorian().strftime("%Y-%m-%d")
    for i in range(len(chart_df))]
chart_df["owner"] = ["davoudian"] * len(chart_df)
chart_df["reference"] = ["کارشناس"] * len(chart_df)
chart_df["name"] = chart_df["commodity"]
chart_df["unit"] = ["-"] * len(chart_df)

names = chart_df["name"].unique().tolist()
for n in tqdm(range(len(names))):
    cursor_ = powerbi_database.cursor()
    cursor_.execute(f"DELETE FROM [nooredenadb].[commodity].[commodities_data] WHERE name='{names[n]}'")
    cursor_.close()

insert_to_database(dataframe=chart_df, database_table="[nooredenadb].[commodity].[commodities_data]")

####################################################################################################################

commodities = ["کرک نوری جدید", "کرک شبندر جدید"]
table = pd.DataFrame()
for i in range(len(commodities)):
    df = pd.read_sql(f"SELECT [date], [price], [commodity] FROM [nooredenadb].[commodity].[commodities_data] where "
                     f"commodity='{commodities[i]}' ORDER BY [date]", powerbi_database)
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
    crsr = powerbi_database.cursor()
    crsr.execute(f"UPDATE [nooredenadb].[commodity].[commodity_data_today] SET price={table['current_price'].iloc[i]}, "
                 f"price_change={table['current_change'].iloc[i]}, "
                 f"change_percent={table['current_change_percent'].iloc[i]} WHERE name='{name_}'")
    crsr.close()
