import pandas as pd
from utils.database import make_connection



def get_symbol_names():

    conn = make_connection()
    symbols = pd.read_sql("SELECT * FROM [nooredenadb].[tsetmc].[symbols]", conn)
    conn.close()
    symbols = symbols.sort_values(
        by=["symbol", "active", "date_", "final_last_date"], ascending=True, ignore_index=True).drop_duplicates(
        subset=["symbol"], keep="last")[["symbol", "symbol_name"]].rename(
        {"symbol": "نماد", "symbol_name": "شرکت"}, axis=1).replace({"ك": "ک", "ي": "ی"}, regex=True)
    return symbols

def _build_query_symbols(n: int) -> str:
    placeholders = ", ".join(["?"] * n)
    return f"""
WITH personnel_latest AS (
    SELECT
        code,
        totalValueOrigin,
        ROW_NUMBER() OVER (PARTITION BY code ORDER BY periodEndingDate DESC) AS rn
    FROM (
        SELECT code, periodEndingDate, SUM(valueOrigin) AS totalValueOrigin
        FROM nooredenadb.bourseview.companies_sga
        WHERE cleanProductName LIKE N'تعداد پرسنل%'
        GROUP BY code, periodEndingDate
    ) AS agg
),
income_summed AS (
    SELECT
        code,
        SUM(CASE WHEN norm_name = N'فروش' THEN ISNULL(valueOrigin, 0) ELSE 0 END) AS sales,
        SUM(CASE WHEN norm_name = N'درامد ارائه خدمات' THEN ISNULL(valueOrigin, 0) ELSE 0 END) AS svcRev,
        SUM(CASE WHEN norm_name = N'سود (زیان) خالص' THEN ISNULL(valueOrigin, 0) ELSE 0 END) AS netIncome
    FROM (
        SELECT
            code,
            valueOrigin,
            REPLACE(REPLACE(REPLACE(statementItemName, N'ي', N'ی'), N'ك', N'ک'), N'آ', N'ا') AS norm_name,
            DENSE_RANK() OVER (PARTITION BY code ORDER BY periodEndingDate DESC) AS period_rk
        FROM nooredenadb.bourseview.companies_income_statement
        WHERE statementItemName IS NOT NULL
    ) AS ranked
    WHERE period_rk <= 4
      AND norm_name IN (N'فروش', N'درامد ارائه خدمات', N'سود (زیان) خالص')
    GROUP BY code
)
SELECT
    s.symbol AS نماد,
    i.industryName AS صنعت,
    pl.totalValueOrigin AS [تعداد پرسنل],
    (isum.sales + isum.svcRev) / 1e13  AS درآمد,
    isum.netIncome / 1e13 AS [سود (زیان) خالص]
FROM       nooredenadb.enigma.symbols s
LEFT  JOIN nooredenadb.enigma.industries i ON s.industryId = i.id
INNER JOIN personnel_latest pl ON pl.code = s.symbolCode AND pl.rn = 1
LEFT  JOIN income_summed isum ON isum.code = s.symbolCode
WHERE i.industryName IN ({placeholders})
ORDER BY i.industryName, s.symbol;
"""

def _build_query_shareholders(n: int) -> str:
    placeholders = ", ".join(["?"] * n)
    return f"""
SELECT
    s.symbol AS نماد,
    i.industryName AS صنعت,
    sh.name AS [نام سهامدار],
    sh.sharePercent AS [٪ سهامداری]
FROM nooredenadb.enigma.shareholders sh
INNER JOIN nooredenadb.enigma.symbols s  ON sh.symbolId   = s.id
LEFT  JOIN nooredenadb.enigma.industries i ON s.industryId  = i.id
WHERE i.industryName IN ({placeholders})
  AND EXISTS (
        SELECT 1
        FROM nooredenadb.bourseview.companies_sga sga
        WHERE sga.code = s.symbolCode
          AND sga.cleanProductName LIKE N'تعداد پرسنل%'
  )
ORDER BY i.industryName, s.symbolCode, sh.sharePercent DESC;
"""

def get_industry_data(industries: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    params = tuple(industries)
    n = len(params)
    conn = make_connection()
    df_symbols = pd.read_sql(_build_query_symbols(n), conn, params=params)
    df_shareholders = pd.read_sql(_build_query_shareholders(n), conn, params=params)
    conn.close()
    return df_symbols, df_shareholders

def save_to_excel_by_industry(
    df_symbols: pd.DataFrame,
    df_shareholders: pd.DataFrame,
    output_dir: str = ".",
) -> None:
    import os
    os.makedirs(output_dir, exist_ok=True)

    industries = df_symbols["صنعت"].dropna().unique()
    symbols = get_symbol_names()

    for industry in industries:
        df_sym_filtered = df_symbols[df_symbols["صنعت"] == industry]
        df_sh_filtered  = df_shareholders[df_shareholders["صنعت"] == industry].drop(columns=['صنعت'])
        df_sh_filtered_top = df_sh_filtered.sort_values(by=["نماد", "٪ سهامداری"], ascending=False).drop_duplicates(
            subset=["نماد"], keep="first")[["نماد", "نام سهامدار", "٪ سهامداری"]]
        df_sym_filtered = df_sym_filtered.merge(df_sh_filtered_top, how="left", on="نماد")

        df_sym_filtered = df_sym_filtered.replace({"ك": "ک", "ي": "ی"}, regex=True).merge(
            symbols, on='نماد', how="left")[
            ["شرکت", "نماد", "تعداد پرسنل", "درآمد", "سود (زیان) خالص", "نام سهامدار", "٪ سهامداری"]]
        df_sh_filtered = df_sh_filtered.replace({"ك": "ک", "ي": "ی"}, regex=True).merge(
            symbols, on='نماد', how="left")[["شرکت", "نماد", "نام سهامدار", "٪ سهامداری"]]

        safe_name = industry.replace("/", "-").replace("\\", "-").replace(":", "-")
        filepath  = os.path.join(output_dir, f"{safe_name}.xlsx")

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df_sym_filtered.to_excel(writer, sheet_name="پرسنل و عملیات",    index=False)
            df_sh_filtered.to_excel(writer,  sheet_name="سهامداران", index=False)

if __name__ == "__main__":
    industry_list = [
        # "فلزات اساسی",
        # "محصولات شیمیایی",
        # "استخراج کانه‌های فلزی",
        # "فراورده‌های نفتی، کک و سوخت هسته‌ای",
        # "مواد و محصولات دارویی",
        # "سیمان، آهک و گچ",
        # "لاستیک و پلاستیک",
        # "خودرو و ساخت قطعات",
        # # "شرکت‌های چندرشته‌ای صنعتی",
        # "محصولات غذایی و آشامیدنی به جز قند و شکر",
        "کاشی و سرامیک",
        "سرمایه‌گذاری‌ها",
        "بانکها و موسسات اعتباری",
        "بیمه و صندوق بازنشستگی به جز تامین اجتماعی"
    ]

    df_sym, df_sh = get_industry_data(industry_list)

    save_to_excel_by_industry(
        df_sym,
        df_sh,
        output_dir="c:/users/h.damavandi/Desktop",
    )
