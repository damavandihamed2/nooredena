import math
from datetime import datetime, timedelta, timezone



def generate_x_app_n(url: str, nt: str, time_diff_ms: int = -2000) -> str:

    if not nt or len(nt) < 3:
        return ""

    t = datetime.now(timezone.utc) + timedelta(milliseconds=time_diff_ms)

    first_two = nt[:2]
    l = nt[2:]

    i = sum(ord(char) for char in url)

    s = (3600 * t.hour) + (60 * t.minute) + t.second

    prefix_val = math.floor(float(first_two))
    offset = abs((s % (len(l) - 5)) - prefix_val)

    sub_val = math.floor(float(l[offset : offset + 5]))

    part1 = str(sub_val * s * i)
    part2 = str(s * i)

    return f"{part1}.{part2}"
