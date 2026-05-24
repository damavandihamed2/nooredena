import datetime
import jdatetime
from dateutil import parser


def _convert_to_miladi(date: str) -> datetime.datetime:
    shamsi_margin = 1500
    t = parser.parse(date)
    if t.year < shamsi_margin:
        t = jdatetime.date(t.year, t.month, t.day).togregorian()
    return t

# Break the time range into 180-day chunks
def _get_time_range(start_date: datetime.datetime, end_date: datetime.datetime, days_range: int = 180) -> list[dict]:
    t_r = []
    while True:
        d_ = start_date + datetime.timedelta(days=days_range - 1)
        d_r = {'start_date': start_date.strftime("%Y-%m-%d"), 'end_date': min(d_, end_date).strftime("%Y-%m-%d")}
        t_r.append(d_r)
        if d_ > end_date:
            break
        start_date = d_ + datetime.timedelta(days=1)
    return t_r


# Handle time: check format, order, and return the list of time ranges
def handle_time(start_date: str, end_date: str) -> list[dict]:
    start_date, end_date = _convert_to_miladi(start_date), _convert_to_miladi(end_date)
    if (end_date - start_date).days < 0: raise ValueError("end_date cant be less than start date!")
    t_r = _get_time_range(start_date, end_date)
    return t_r


if __name__ == '__main__':
    time_range = handle_time(start_date="1403-06-31", end_date="1405-03-03")

