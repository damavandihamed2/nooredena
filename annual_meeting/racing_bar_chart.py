import arabic_reshaper
import bar_chart_race as bcr
from bidi.algorithm import get_display

from annual_meeting.utils.data import get_sectors_value_daily


def fix_persian_text(text: str):
    fixed_text = get_display(arabic_reshaper.reshape(text))
    return fixed_text

def summary(values, ranks):
    total_value = int(round(values.sum(), -2))
    s = f"{total_value: ,.0f}" + fix_persian_text("ارزش پرتفولیو: ")
    return {'x': .99, 'y': .05, 's': s, 'ha': 'right', 'size': 12}


df = get_sectors_value_daily("1404/09/30", "1404/10/30")
df.columns = [fix_persian_text(c) for c in df.columns]
df.index = [fix_persian_text(i) for i in df.index]

bcr.bar_chart_race(
    df,
    figsize=(10, 6),
    n_bars=15,
    filename="c:/users/h.damavandi/desktop/racing_chart_sectors.mp4",

    steps_per_period=20,
    period_length=200,
    interpolate_period=True,

    label_bars=True,
    bar_label_size=8,


    period_label={'x': .99, 'y': .1, 'ha': 'right', 'color': 'red', 'family': 'B Mitra'},    # or True
    tick_label_size=10,

    title=fix_persian_text('روند تغییرات ارزش صنایع در پورتفولیو (مبالغ به میلیارد ریال می باشد)'),
    title_size='x-large',
    shared_fontdict={'family': 'B Mitra', 'weight': 'bold', 'color': 'rebeccapurple'},

    # period_fmt='%Y/%m',
    period_summary_func=summary,
)
