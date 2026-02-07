import re
import pyodbc
import datetime
import jdatetime
from dataclasses import dataclass

@dataclass(frozen=True)
class ParsedDate:
    y: int
    m: int
    d: int

def _parse_flexible_date(x) -> ParsedDate:
    """
    Accepts:
      - int: 20250101 / 14040101
      - str: '2025-1-5', '2025/01/05', '20250101', '1404-01-05', ...
    Returns y,m,d as ints.
    """
    if x is None:
        raise ValueError("date is required")

    s = str(x).strip()

    # If it's exactly 8 digits -> YYYYMMDD
    m8 = re.fullmatch(r"\d{8}", s)
    if m8:
        y = int(s[0:4]); m = int(s[4:6]); d = int(s[6:8])
        return ParsedDate(y, m, d)

    # Extract all numeric groups (works for '-', '/', spaces, etc.)
    parts = re.findall(r"\d+", s)
    if len(parts) != 3:
        raise ValueError(f"Invalid date format: {x!r}. Expected YYYYMMDD or YYYY-MM-DD or YYYY/MM/DD")

    y, m, d = map(int, parts)
    return ParsedDate(y, m, d)

def _jalali_to_gregorian(jy: int, jm: int, jd: int) -> ParsedDate:
    """
    Jalali (Persian) -> Gregorian conversion.
    """

    d_ = jdatetime.datetime(jy, jm, jd).togregorian()

    gy, gm, gd = d_.year, d_.month, d_.day

    return ParsedDate(gy, gm, gd)

def normalize_to_gregorian_yyyymmdd(x) -> int:
    """
    Returns Gregorian date as int YYYYMMDD.
    Heuristic: year < 1700 => Jalali, else Gregorian.
    """
    p = _parse_flexible_date(x)

    if not (1 <= p.m <= 12 and 1 <= p.d <= 31):
        raise ValueError(f"Invalid month/day in date: {x!r}")

    # Heuristic: Jalali years are typically 13xx/14xx; Gregorian 19xx/20xx
    if p.y < 1700:
        g = _jalali_to_gregorian(p.y, p.m, p.d)
    else:
        g = p

    return g.y * 10000 + g.m * 100 + g.d


def get_last_date(broker_type: str, db_style: bool, db_conn: pyodbc.Connection) -> str:
    if broker_type == "omex":
        crsr = db_conn.cursor()
        crsr.execute("SELECT MAX(eDate) FROM [nooredenadb].[brokers].[option_settlements_omex]")
        d = crsr.fetchall()[0][0][:10]
        crsr.close()
        if not db_style:
            d = datetime.datetime.strptime(d, "%Y-%m-%d")
            d = jdatetime.datetime.fromgregorian(
                year=d.year, month=d.month, day=d.day).strftime("%Y/%m/%d")
    elif broker_type == "online_plus":
        crsr = db_conn.cursor()
        crsr.execute("SELECT MAX(SettlementDate) FROM [nooredenadb].[brokers].[option_settlements_online_plus]")
        d = crsr.fetchall()[0][0]
        crsr.close()
        if not db_style:
            d = d.replace("-", "/")
    else:
        raise ValueError(f"Invalid broker type: {broker_type}")
    return d
