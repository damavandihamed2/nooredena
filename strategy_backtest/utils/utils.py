import typing, math, datetime, jdatetime


def get_commission_fee(yval: str, trade_type: typing.Literal[1, 2]) -> float:
    commission_fee = {
        "300": {1: 0.003712, 2: 0.0088}, # bourse
        "303": {1: 0.003632, 2: 0.0088}, # farabourse
        "309": {1: 0.003632, 2: 0.0088}, # farabourse
        "313": {1: 0.003632, 2: 0.0088}, # farabourse
        "305": {1: 0.00116, 2: 0.0011875}, # fund
        "380": {1: 0.00125, 2: 0.00125},  # Gold fund
    }
    return commission_fee[yval][trade_type]


def buy(
        date: int,
        current_price: int,
        balance: int,
        available_value: int,
        asset_type: str,
        strategy: str
) -> dict[str, int]:

    commission = get_commission_fee(yval=asset_type, trade_type=1)

    if math.ceil(available_value * (1 + commission)) < balance:
        volume = available_value // (current_price * (1 + commission))
    else:
        volume = balance // (current_price * (1 + commission))
    value_gross = volume * current_price
    value_commission = math.ceil(commission * value_gross)
    value_net = value_gross + value_commission
    balance = balance - value_net
    return {
        "date": date,
        "type": 1,
        "strategy": strategy,
        "price": current_price,
        "volume": volume,
        "value_gross": value_gross,
        "commission": value_commission,
        "value_net": value_net,
        "balance": balance
    }

def sell(
        date: int,
        entry_price: int,
        balance: int,
        current_price: int,
        volume: int,
        available_value: int,
        asset_type: str,
        strategy: str
) -> dict[str, int]:

    commission = get_commission_fee(yval=asset_type, trade_type=2)

    if volume * current_price < available_value:
        volume = volume
    else:
        volume = available_value // current_price
    value_gross = volume * current_price
    value_commission = math.ceil(commission * value_gross)
    value_net = value_gross - value_commission
    balance = balance + value_net
    pnl_gross = value_gross - (entry_price * volume)
    pnl_net = value_net - (entry_price * volume)
    return {
        "date": date,
        "type": 2,
        "strategy": strategy,
        "price": current_price,
        "volume": volume,
        "value_gross": value_gross,
        "commission": value_commission,
        "value_net": value_net,
        "entry_price": entry_price,
        "pnl_gross": pnl_gross,
        "pnl_net": pnl_net,
        "balance": balance
    }


def date_input_handler(
        input_date
):
    if type(input_date) == str:
        pass
    elif (type(input_date) == int) or (type(input_date) == float):
        if type(input_date) == float:
            if input_date % 1 > 0:
                raise ValueError("date must be an Integer or String!")
            else:
                input_date = int(input_date)
        input_date = str(input_date)
    else:
        raise ValueError("date must be an Integer or String!")

    input_date = input_date.replace("-", "").replace("/", "")
    if int(input_date) > 15000000:
        input_date = datetime.datetime.strptime(input_date, "%Y%m%d")
        if input_date > datetime.datetime.now():
            raise ValueError("date Could not be in the future")
        else:
            input_date = input_date.strftime("%Y%m%d")
            input_date = int(input_date)
    else:
        input_date = jdatetime.datetime.strptime(input_date, "%Y%m%d")
        if input_date > jdatetime.datetime.now():
            raise ValueError("date Could not be in the future")
        else:
            input_date = input_date.togregorian()
            input_date = input_date.strftime("%Y%m%d")
            input_date = int(input_date)
    return input_date

