import numpy as np
import pandas as pd
from typing import Literal


def trigger(date: int, trigger_type: Literal[1, 2], strategy: str, entry_price: int,
            stop_loss: int = None, exit_price: int = None, profit_pct: float = None) -> dict:
    if trigger_type == 1:
        return {"date": date, "type": trigger_type, "strategy": strategy,
                "entry_price": entry_price, "stop_loss": stop_loss}
    elif trigger_type == 2:
        return {"date": date, "type": trigger_type, "strategy": strategy,
                "entry_price": entry_price, "exit_price": exit_price, "profit_pct": profit_pct}
    else:
        raise ValueError("trigger_type must be 1 or 2")

def fixed_trigger(trigger_type: Literal[1, 2], strategy: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return trigger(trigger_type=trigger_type, strategy=strategy, *args, **kwargs)
        return wrapper
    return decorator

@fixed_trigger(trigger_type=1, strategy="RSI")
def trigger_buy_rsi(date: int, entry_price: int, stop_loss: int):
    return {"date": date, "entry_price": entry_price, "stop_loss": stop_loss}
@fixed_trigger(trigger_type=1, strategy="RSI-Wilder")
def trigger_buy_rsi_wilder(date: int, entry_price: int, stop_loss: int):
    return {"date": date, "entry_price": entry_price, "stop_loss": stop_loss}
@fixed_trigger(trigger_type=1, strategy="LAST TOP")
def trigger_buy_last_stop(date: int, entry_price: int, stop_loss: int):
    return {"date": date, "entry_price": entry_price, "stop_loss": stop_loss}

@fixed_trigger(trigger_type=2, strategy="STOP LOSS")
def trigger_sell_stop_loss(date: int, entry_price: int, exit_price: int, profit_pct: float):
    return {"date": date, "entry_price": entry_price, "exit_price": exit_price, "profit_pct": profit_pct}
@fixed_trigger(trigger_type=2, strategy="EMA TRAILING STOP")
def trigger_sell_ema_trailing(date: int, entry_price: int, exit_price: int, profit_pct: float):
    return {"date": date, "entry_price": entry_price, "exit_price": exit_price, "profit_pct": profit_pct}


def calculate_rsi(data: pd.DataFrame, rsi_window: int = 14, wilder_smoothing: bool = False) -> np.ndarray :
    delta = data['close_price'].diff()
    gain = delta.clip(lower=0)  # معادل delta.where(delta>0, 0)
    loss = (-delta).clip(lower=0)  # معادل -delta.where(delta<0, 0)
    # calculating initial SMA for first {rsi_window} periods
    avg_gain = gain.rolling(rsi_window, min_periods=rsi_window).mean()
    avg_loss = loss.rolling(rsi_window, min_periods=rsi_window).mean()
    if wilder_smoothing:
        # Smoothing RSI with alpha = 1 / {rsi_window}
        avg_gain = avg_gain.combine_first(gain.ewm(alpha=1 / rsi_window, adjust=False).mean())
        avg_loss = avg_loss.combine_first(loss.ewm(alpha=1 / rsi_window, adjust=False).mean())
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = np.where(avg_loss == 0, 100, rsi)
    return rsi



def signal(instrument_df: pd.DataFrame, rsi_period: int = 14, wilder_smoothing: bool = False, ema_period: int = 20,
           # use_trailing_stop: bool = False,
           trailing_stop_pct: int = 10, stop_loss_pct: int = 10) -> dict :
    """
    param
    instrument_df:
        DataFrame with columns (date, open_price, high_price, low_price, close_price)
    rsi_period:
        the window for calculating the RSI indicator (default is 14 days)
    use_trailing_stop:
        Flag for switching to EMA20 exit after trailing_stop_pct% profit (default is False)
    trailing_stop_pct:
        the profit percent that activates trailing stop (default is 10 higher)
    stop_loss_pct:
        the loss percent that activates stop loss (default is 10 lower than entry price)

    return
    Dictionary with triggers, modified dataframe and parameters of strategy
    """
    # Initialize variables
    last_top = 0
    in_position = False
    entry_price = 0
    stop_loss = 0
    triggers = []
    rsi_oversold_days = 0
    use_trailing_stop = False

    # Prepare data and indicators EMA (close_price), RSI (period = rsi_period)
    df = instrument_df.copy()
    df.sort_values('date', inplace=True, ignore_index=True)
    df[f"ema{ema_period}"] = df["close_price"].ewm(span=ema_period).mean()
    df["rsi"] = calculate_rsi(df, rsi_window=rsi_period, wilder_smoothing=wilder_smoothing)

    for i in range(25, len(df)):

        current_close = df["close_price"].iloc[i]
        prev_close = df["close_price"].iloc[i - 1]
        ema_yesterday = df[f"ema{ema_period}"].iloc[i - 1]

        if in_position:
            current_profit = ((current_close - entry_price) / entry_price) * 100

            # Check for initial stop loss (stop_loss_pct% below entry)
            if current_close <= stop_loss:
                in_position = False
                use_trailing_stop = False
                rsi_oversold_days = 0
                current_trigger = trigger_sell_stop_loss(
                    date=df["date"].iloc[i],
                    entry_price=entry_price,
                    exit_price=current_close,
                    profit_pct=round((current_close - entry_price) / entry_price * 100, 2)
                )
                triggers.append(current_trigger)
                continue

            # Check if price has increased {trailing_stop_pct}% to activate trailing stop
            if not use_trailing_stop and current_profit >= trailing_stop_pct:
                use_trailing_stop = True

            # EMA20 trailing stop condition (only after {trailing_stop_pct}% profit)
            if use_trailing_stop and prev_close < ema_yesterday:
                in_position = False
                use_trailing_stop = False
                rsi_oversold_days = 0
                current_trigger = trigger_sell_ema_trailing(
                    date=df["date"].iloc[i],
                    entry_price=entry_price,
                    exit_price=current_close,
                    profit_pct=round(current_profit, 2)
                )
                triggers.append(current_trigger)
                continue

            # Update last_top price
            last_top = max(last_top, current_close)

        if not in_position:
            # RSI Strategy
            current_rsi = df["rsi"].iloc[i - 1]
            prev_rsi = df["rsi"].iloc[i - 2] if i > 1 else 50

            if current_rsi < 30:
                rsi_oversold_days += 1

            else:
                if (rsi_oversold_days >= 2) and (prev_rsi < 30) and (current_rsi >= 30):
                    in_position = True
                    entry_price = current_close
                    use_trailing_stop = False
                    rsi_oversold_days = 0
                    stop_loss = int(entry_price * (1 - (stop_loss_pct / 100)))  # stop_loss_pct% stop loss
                    if wilder_smoothing:
                        current_trigger = trigger_buy_rsi_wilder(
                            date=df["date"].iloc[i], entry_price=current_close, stop_loss=stop_loss_pct)
                    else:
                        current_trigger = trigger_buy_rsi(
                            date=df["date"].iloc[i], entry_price=current_close, stop_loss=stop_loss_pct)
                    triggers.append(current_trigger)

                else:
                    rsi_oversold_days = 0

            # LAST TOP Strategy
            if (current_close > last_top) and (last_top != 0):
                in_position = True
                entry_price = current_close
                use_trailing_stop = False
                rsi_oversold_days = 0
                stop_loss = int(entry_price * (1 - (stop_loss_pct / 100)))  # 10% stop loss
                current_trigger = trigger_buy_last_stop(
                    date=df["date"].iloc[i], entry_price=current_close)
                triggers.append(current_trigger)

    return {"triggers": triggers,
            "instrument_df": df,
            "parameters": {"rsi_period": rsi_period,
                           "use_trailing_stop": use_trailing_stop,
                           "initial_stop_loss_pct": stop_loss_pct,
                           "profit_threshold_for_trailing": trailing_stop_pct,
                           "trailing_stop_method": "EMA20"}}



def signal_old(instrument_df: pd.DataFrame) -> dict :
    """
    :param instrument_df: DataFrame with columns (date(gregorian date with int type) - open, high, low and close price)
    :return: list of dictionaries, each one indicates a trigger (buy:1 or sell:2) in a specific day and it's strategy
    """
    last_top = 0
    in_position = False
    triggers = []

    # Prepare data and indicators (EMA9 on mean(low_price, high_price), EMA20 on close_price, EMA25 on high_price)
    instrument_df.sort_values('date', inplace=True, ignore_index=True, ascending=True)
    instrument_df["ema9"] = ((instrument_df["high_price"] + instrument_df["low_price"]) / 2).ewm(span=9).mean()
    instrument_df["ema20"] = instrument_df["high_price"].ewm(span=20).mean()
    instrument_df["ema25"] = instrument_df["high_price"].ewm(span=25).mean()

    for i in range(25, len(instrument_df)):
        if in_position:
            if instrument_df["close_price"].iloc[i-1] < instrument_df["ema20"].iloc[i-1]:
                # sell signal
                triggers.append({"date": instrument_df["date"].iloc[i], "type": 2, "strategy": "EMA"})
                in_position = False
            else:
                # update last top price
                last_top = max(last_top, instrument_df["close_price"].iloc[i])
        if not in_position:
            if ((instrument_df["ema9"].iloc[i - 1] > instrument_df["ema25"].iloc[i - 1]) and
                    (instrument_df["ema9"].iloc[i - 2] <= instrument_df["ema25"].iloc[i - 2])):
                # buy total index
                triggers.append({"date": instrument_df["date"].iloc[i], "type": 1, "strategy": "EMA"})
                in_position = True
            elif (instrument_df["close_price"].iloc[i-1] > last_top) and (last_top != 0):
                # buy total index
                triggers.append({"date": instrument_df["date"].iloc[i], "type": 1, "strategy": "LAST TOP"})
                in_position = True
            else:
                pass
    return {"triggers": triggers, "instrument_df": instrument_df}




def calculate_rsi_wilder(data, window=14):
    """محاسبه RSI با روش Wilder's Smoothing"""
    delta = data['close_price'].diff()

    gain = delta.clip(lower=0)  # معادل delta.where(delta>0, 0)
    loss = (-delta).clip(lower=0)  # معادل -delta.where(delta<0, 0)

    # محاسبه SMA اولیه برای 14 دوره اول
    avg_gain = gain.rolling(window, min_periods=window).mean()
    avg_loss = loss.rolling(window, min_periods=window).mean()

    # اعمال Wilder's Smoothing (EWM با alpha=1/14)
    avg_gain = avg_gain.combine_first(gain.ewm(alpha=1 / window, adjust=False).mean())
    avg_loss = avg_loss.combine_first(loss.ewm(alpha=1 / window, adjust=False).mean())

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi = np.where(avg_loss == 0, 100, rsi)

    return rsi


def signal_old_2(instrument_df: pd.DataFrame) -> dict:
    """
    :param instrument_df: DataFrame با ستون‌های (date, open_price, high_price, low_price, close_price)
    :return: دیکشنری شامل سیگنال‌ها و دیتافریم اصلاح شده
    """
    # متغیرهای اولیه
    last_top = 0
    in_position = False
    entry_price = 0
    stop_loss = 0
    triggers = []
    rsi_oversold_days = 0
    use_trailing_stop = False

    # آماده‌سازی داده‌ها
    df = instrument_df.copy()
    df.sort_values('date', inplace=True, ignore_index=True)

    # محاسبه اندیکاتورها
    df["ema20"] = df["close_price"].ewm(span=20).mean()
    df["ema25"] = df["high_price"].ewm(span=25).mean()

    # محاسبه RSI با روش Wilder
    df["rsi"] = calculate_rsi_wilder(df, window=14)

    for i in range(25, len(df)):
        current_close = df["close_price"].iloc[i]
        prev_close = df["close_price"].iloc[i - 1]
        ema20_yesterday = df["ema20"].iloc[i - 1]

        if in_position:
            current_profit = (current_close - entry_price) / entry_price * 100

            # بررسی حد ضرر اولیه (10% زیر قیمت ورود)
            if current_close <= stop_loss:
                triggers.append({
                    "date": df["date"].iloc[i],
                    "type": 2,
                    "strategy": "STOP LOSS",
                    "entry_price": entry_price,
                    "exit_price": current_close,
                    "profit_pct": round((current_close - entry_price) / entry_price * 100, 2)
                })
                in_position = False
                use_trailing_stop = False
                rsi_oversold_days = 0
                continue

            # فعال شدن حد ضرر متغیر پس از 10% سود
            if not use_trailing_stop and current_profit >= 10:
                use_trailing_stop = True

            # شرط خروج با EMA20 (فقط پس از 10% سود)
            if use_trailing_stop and prev_close < ema20_yesterday:
                triggers.append({
                    "date": df["date"].iloc[i],
                    "type": 2,
                    "strategy": "EMA TRAILING STOP",
                    "entry_price": entry_price,
                    "exit_price": current_close,
                    "profit_pct": round(current_profit, 2)
                })
                in_position = False
                use_trailing_stop = False
                rsi_oversold_days = 0
                continue

            # به روزرسانی آخرین سقف قیمتی
            last_top = max(last_top, current_close)

        if not in_position:
            # استراتژی RSI با روش Wilder
            current_rsi = df["rsi"].iloc[i - 1]
            prev_rsi = df["rsi"].iloc[i - 2] if i > 1 else 50

            if current_rsi < 30:
                rsi_oversold_days += 1
            else:
                if (rsi_oversold_days >= 2) and (prev_rsi < 30) and (current_rsi >= 30):
                    triggers.append({
                        "date": df["date"].iloc[i],
                        "type": 1,
                        "strategy": "RSI-Wilder",
                        "entry_price": current_close
                    })
                    in_position = True
                    entry_price = current_close
                    stop_loss = entry_price * 0.9  # حد ضرر 10%
                    use_trailing_stop = False
                    rsi_oversold_days = 0
                else:
                    rsi_oversold_days = 0

            # استراتژی LAST TOP (حفظ شده)
            if (current_close > last_top) and (last_top != 0):
                triggers.append({
                    "date": df["date"].iloc[i],
                    "type": 1,
                    "strategy": "LAST TOP",
                    "entry_price": current_close
                })
                in_position = True
                entry_price = current_close
                stop_loss = entry_price * 0.9  # حد ضرر 10%
                use_trailing_stop = False
                rsi_oversold_days = 0

    return {
        "triggers": triggers,
        "instrument_df": df,
        "parameters": {
            "rsi_type": "Wilder's Smoothing",
            "rsi_window": 14,
            "initial_stop_loss_pct": 10,
            "profit_threshold_for_trailing": 10,
            "trailing_stop_method": "EMA20"
        }
    }
