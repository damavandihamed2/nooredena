from datetime import datetime, timedelta
from dateutil import parser
import jdatetime


def _time_format(date: str|datetime) -> str:
    if type(date)==str:
        date = parser.parse(date)
    return date.strftime("%Y-%m-%d")

# If the date is shamsi, convert it to miladi
def _convert_to_miladi(date: str) -> datetime:
    shamsi_margin = 1500
    t = parser.parse(date)
    if t.year < shamsi_margin:
        t = jdatetime.date(t.year, t.month, t.day).togregorian()
    return t


# Calculate the time difference between the start and end dates
def _get_time_diff(s_date: datetime, e_date: datetime) -> tuple[int, datetime]:
    difference = (e_date - s_date).days
    return difference, s_date




# Break the time range into 180-day chunks
def get_time_range(s_date: str, e_date: str, days_range: int = 180) -> list[dict]:

    s_date = "1404/01/01"
    e_date = "1405/02/03"

    time_range = []
    while True:
        difference, date1 = _get_time_diff(s_date, e_date)

        if difference < days_range:
            time_range.append({'s_date': _time_format(s_date), 'e_date': _time_format(e_date)})
            break;

        e_date1 = date1 + timedelta(days=days_range - 1)
        e_date1 = _time_format(e_date1)

        time_range = time_range + [{'s_date': _time_format(s_date), 'e_date': e_date1}]

        s_date = date1 + timedelta(days=days_range)
        s_date = _time_format(s_date)

    return time_range


# Handle time: check format, order, and return the list of time ranges
def handle_time(s_date: str, e_date: str) -> list[dict]:
    s_date = _convert_to_miladi(s_date)
    e_date = _convert_to_miladi(e_date)
    time_diff, _ = _get_time_diff(s_date, e_date)
    if time_diff < 0:
        print('بازه انتخابی اشتباه است')
        return
    return get_time_range(s_date, e_date)

