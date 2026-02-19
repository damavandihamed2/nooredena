import os
import warnings
import pandas as pd

from utils.database import make_connection


warnings.filterwarnings("ignore")
db_conn = make_connection()
symbols_detail_data = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols_detail_data]", db_conn)


def get_sheet_name(filepath):
    sheets = pd.ExcelFile(filepath).sheet_names
    sheet_name = None
    if "سهام" in sheets:
        sheet_name = "سهام"
    elif "1" in sheets:
        sheet_name = "1"
    else:
        for s in range(len(sheets)):
            if "سهام" in sheets[s]:
                sheet_name = sheets[s]
                break
    return sheet_name


def filereader1(filepath, sheetname, fundname):
    df = pd.read_excel(filepath, sheet_name=sheetname)
    df.replace({"\u202b": ""}, inplace=True, regex=True)
    first_col = df.columns[0]
    date_ = df[first_col].iloc[1].split()[-1]
    if len(df.index[df[first_col] == "شرکت"]) != 0:
        idx_ = df.index[df[first_col] == "شرکت"].values[0]
    else:
        idx_ = df.index[df[first_col] == "نام شرکت"].values[0]
    filtering_rows = df[first_col].iloc[:idx_ + 1].unique().tolist() + [first_col] + ["جمع", "مجموع"]
    df = df.iloc[idx_:, :]
    df.reset_index(drop=True, inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df_t = df.T
    amount_0_idx = df_t.index[df_t[0] == "تعداد"].values[0]
    amount_1_idx = df_t.index[df_t[0] == "تعداد"].values[-1]
    df = df[[first_col, amount_0_idx, amount_1_idx]]
    df = df[~df[first_col].isin(filtering_rows)]
    df.columns = ["company", "amount0", "amount1"]
    df.dropna(subset="company", inplace=True, ignore_index=True)
    df.fillna(0, inplace=True)
    df["fund"] = [fundname] * len(df)
    df["date"] = [date_] * len(df)

    return df


#######################################################################################################################
#######################################################################################################################

funds_directory = "//filesrv/Public/گزارش روزانه/Backup/funds/"
funds_names = os.listdir(funds_directory)

tempdf = pd.DataFrame()
for fn in range(len(funds_names)):
    files_names = os.listdir(funds_directory + funds_names[fn])
    for n in range(len(files_names)):
        file_dir = funds_directory + funds_names[fn] + "/" + files_names[n]
        sheet_name = get_sheet_name(file_dir)
        if sheet_name == "1":
            tmp = filereader1(filepath=file_dir, sheetname=sheet_name, fundname=funds_names[fn])
            tempdf = pd.concat([tempdf, tmp], axis=0,  ignore_index=True)
            tempdf["company"] = tempdf["company"].str.replace("- (نماد قدیمی حذف شده)", "")



del_idx = []
for i in range(len(tempdf)-1):
    if tempdf["company"].iloc[i] == tempdf["company"].iloc[i+1]:
        tempdf["amount0"].iloc[i] = tempdf["amount0"].iloc[i] + tempdf["amount0"].iloc[i+1]
        tempdf["amount1"].iloc[i] = tempdf["amount1"].iloc[i] + tempdf["amount1"].iloc[i+1]
        del_idx.append(i+1)
tempdf.drop(labels=del_idx, axis=0, inplace=True)
tempdf.reset_index(drop=True, inplace=True)

tempdf["symbol"] = ["-"] * len(tempdf)
tempdf["symbol_name"] = ["-"] * len(tempdf)

for i in range(len(tempdf)):
    company = tempdf["company"].iloc[i].replace("ی", "ي").replace("ک", "ك")
    if company in symbols_detail_data["symbol_name"].values:
        tempdf["symbol_name"].iloc[i] = company
    if company in symbols_detail_data["symbol"].values:
        tempdf["symbol"].iloc[i] = company






funds_directory = "//filesrv/Public/گزارش روزانه/Backup/funds/"
funds_names = os.listdir(funds_directory)

tempdf = pd.DataFrame()
for fn in range(len(funds_names)):
    files_names = os.listdir(funds_directory + funds_names[fn])
    for n in range(len(files_names)):
        file_dir = funds_directory + funds_names[fn] + "/" + files_names[n]
        sheet_name = get_sheet_name(file_dir)
        tempdf = pd.concat([tempdf, pd.DataFrame({"filepath": [file_dir], "sheet_name": [sheet_name]})],
                           axis=0,
                           ignore_index=True)

file_types = tempdf["sheet_name"].unique().tolist()

type1 = tempdf[tempdf["sheet_name"] == "1"]
type1.reset_index(drop=True, inplace=True)
type2 = tempdf[tempdf["sheet_name"] == "سهام"]
type2.reset_index(drop=True, inplace=True)
type3 = tempdf[tempdf["sheet_name"] == "سرمایه گذاری در سهام"]
type3.reset_index(drop=True, inplace=True)


for i in range(len(type1)):

    df = pd.read_excel(type1["filepath"].iloc[i], type1["sheet_name"].iloc[i])
    df.replace({"\u202b": ""}, inplace=True, regex=True)
    first_col = df.columns[0]
    if len(df.index[df[first_col] == "شرکت"]) != 0:
        idx_ = df.index[df[first_col] == "شرکت"].values[0]
    else:
        idx_ = df.index[df[first_col] == "نام شرکت"].values[0]
    filtering_rows = df[first_col].iloc[:idx_+1].unique().tolist() + [first_col] + ["جمع", "مجموع"]
    df = df.iloc[idx_:, :]
    df.reset_index(drop=True, inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df_t = df.T
    amount_0_idx = df_t.index[df_t[0] == "تعداد"].values[0]
    amount_1_idx = df_t.index[df_t[0] == "تعداد"].values[-1]
    df = df[[first_col, amount_0_idx, amount_1_idx]]
    df = df[~df[first_col].isin(filtering_rows)]
    df.columns = ["company", "amount0", "amount1"]
    df.dropna(subset="company", inplace=True, ignore_index=True)
    df.fillna(0, inplace=True)



"صندوق قابل معامله بخشی پتروشیمی دماوند"
"صورت وضعیت پورتفوی"
"برای ماه منتهی به 1402/02/31"
"شرکت"
"نقل از صفحه قبل"
"نقل به صفحه بعد"

df[first_col].isin(filtering_rows + [first_col])



df[first_col].iloc[0] + str(df[first_col].iloc[1])


for fn in range(len(funds_names)):
    fn = 1
    files_names = os.listdir(funds_directory + funds_names[fn])
    for n in range(len(files_names)):
        n = 0
        file_dir = funds_directory + funds_names[fn] + "/" + files_names[n]
        sheet_name = get_sheet_name(file_dir)
        if sheet_name != None:
            df = pd.read_excel(file_dir, sheet_name=sheet_name)
            df.replace({"\u202b": ""}, inplace=True, regex=True)
            first_col = df.columns[0]
            if len(df.index[df[first_col] == "شرکت"]) != 0:
                idx_ = df.index[df[first_col] == "شرکت"].values[0]
            else:
                idx_ = df.index[df[first_col] == "نام شرکت"].values[0]
            df = df.iloc[idx_:, :]
            df.dropna(subset=first_col, axis=0, inplace=True, ignore_index=True)




file_dir = "//filesrv/Public/گزارش روزانه/Backup/funds/آگاس/1402.01.31.xlsx"
sheet_name = "1"
df = pd.read_excel(file_dir, sheet_name=sheet_name)
df.replace({"\u202b": ""}, inplace=True, regex=True)
first_col = df.columns[0]
if len(df.index[df[first_col] == "شرکت"]) != 0:
    idx_ = df.index[df[first_col] == "شرکت"].values[0]
else:
    idx_ = df.index[df[first_col] == "نام شرکت"].values[0]
df = df.iloc[idx_:, :]
df.dropna(subset=first_col, axis=0, inplace=True, ignore_index=True)

###################################################################################################

symbols = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols_detail_data]", db_conn)
symbols = symbols[["symbol", "sector_name"]]
symbols.rename(mapper={"symbol": "نماد", "sector_name": "صنعت"}, axis=1, inplace=True)
dd = pd.read_excel("C:/Users/damavandi/Desktop/chemicals.xlsx")
dd["نماد"].replace("ک", "ك", regex=True, inplace=True)
dd["نماد"].replace("ی", "ي", regex=True, inplace=True)

x = dd.copy()
x["صنعت"] = [""] * len(x)
for i in range(len(x)):
    if x["نماد"].iloc[i] in symbols["نماد"].values:
        idx = symbols.index[symbols["نماد"] == x["نماد"].iloc[i]].values[0]
        x["صنعت"].iloc[i] = symbols["صنعت"].iloc[idx]

x.to_excel("C:/Users/damavandi/Desktop/chemicals_new.xlsx", index=False)
# x = dd.merge(symbols, on="نماد", how="left")

