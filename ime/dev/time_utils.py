from datetime import datetime, timedelta
from dateutil import parser
import jdatetime

    # Calculate the time difference between the start and end dates
def get_time_diff(s_date: str, e_date: str) -> tuple[int, datetime]:
    date1 = parser.parse(s_date)
    date2 = parser.parse(e_date)
    difference = date2 - date1
    return difference.days, date1

def _time_format(date: str|datetime) -> datetime:
    if type(date)==str:
        date = parser.parse(date)
    return date.strftime("%Y-%m-%d")

# If the date is shamsi, convert it to miladi
def convert_to_miladi(date: str) -> str:
    shamsi_margin = 1500
    t = parser.parse(date)
    y, m, d = t.year, t.month, t.day
    if y < shamsi_margin:
        gregorian = jdatetime.date(y, m, d).togregorian()
        date = f"{gregorian.year}-{gregorian.month:02d}-{gregorian.day:02d}"
    return date
    
# Break the time range into 180-day chunks
def get_time_range(s_date: str, e_date: str) -> list[dict]:
    
    s_date, e_date = handle_time(s_date, e_date)
    if not(s_date and e_date):
        return []
    six_months = 180
    time_range = []
    while True:
        difference, date1 = get_time_diff(s_date, e_date)
        if difference < six_months:
            time_range.append({'s_date': _time_format(s_date), 'e_date': _time_format(e_date)})
            break;
        e_date1 = date1 + timedelta(days = (six_months-1))
        e_date1 = _time_format(e_date1)
        time_range.append({'s_date': _time_format(s_date), 'e_date': e_date1})
        s_date = date1 + timedelta(days = six_months)
        s_date = _time_format(s_date)
    return time_range

# Handle time: check format, order, and return the list of time ranges
def handle_time(s_date: str, e_date: str) -> tuple[str, str]:
    s_date = convert_to_miladi(s_date)
    e_date = convert_to_miladi(e_date)
    time_diff, _ = get_time_diff(s_date, e_date)
    if time_diff < 0:
        print('بازه انتخابی اشتباه است')
        return None, None
    return s_date, e_date

