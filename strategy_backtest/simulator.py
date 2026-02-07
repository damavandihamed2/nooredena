import pandas as pd
import tqdm, math, warnings, typing, pathlib

from simulation.utils.data_func import get_asset_history, get_index_symbols, get_symbols_data
from simulation.utils.utils import date_input_handler, buy, sell
from simulation.utils.signal_func import signal


warnings.filterwarnings("ignore")


def get_strategy_return(
        initial_balance: int,
        from_date: typing.Optional[int],
        asset_id_signal: str,
        asset_id_trade: str,
        asset_type: str,
        buy_fix_income: bool,
        max_share_of_market_trade: float,
) -> dict[str, float]:

    # initial_balance = 10_000_000_000
    # from_date = 1400_01_01
    # asset_id_signal = "67130298613737946"
    # asset_id_trade = "31316784129157162"
    # asset_type = "309"
    # buy_fix_income = True
    # max_share_of_market_trade = 0.1

    asset_signal = get_asset_history(asset_id_signal, adj_price=True)
    signals = signal(
        instrument_df=asset_signal,
        rsi_period=17,
        wilder_smoothing=True,
        trailing_stop_pct=14,
        stop_loss_pct=10,
        ema_period=11
    )
    signals_df = pd.DataFrame(signals["triggers"])
    if len(signals_df) == 0:
        signals_df = pd.DataFrame(
            columns=["date", "type", "strategy", "entry_price", "exit_price", "profit_pct", "stop_loss"])
        signal_asset_df = pd.DataFrame()
    else:
        signals_df = signals_df.sort_values(by=["date"], inplace=False, ignore_index=True)
        signal_asset_df = signals["instrument_df"]

    asset_trade = get_asset_history(asset_id_trade, adj_price=True)

    asset_trade = asset_trade.merge(signals_df, on="date", how="outer")
    asset_trade.dropna(subset=["close_price"], inplace=True, ignore_index=True)

    if from_date:
        from_date = date_input_handler(from_date)
        asset_trade = asset_trade[asset_trade["date"] > from_date]
        asset_trade = asset_trade.sort_values(by=["date"], ascending=True, inplace=False, ignore_index=True)
    else:
        asset_trade.sort_values(by=["date"], ascending=True, inplace=True, ignore_index=True)

    buy_hold_amount = (initial_balance // asset_trade["close_price"].iloc[0])

    asset_trade["b&h_balance"] = buy_hold_amount * asset_trade["close_price"]
    asset_trade["strategy_cash"] = 0
    asset_trade["strategy_cash"].iloc[0] = initial_balance
    asset_trade["strategy_amount"] = 0

    trade_table = []
    entry_price = None
    strategy_status = None
    strategy_name = None
    for i in range(1, len(asset_trade)):

        date_ = asset_trade["date"].iloc[i]
        balance = asset_trade["strategy_cash"].iloc[i - 1]
        amount = asset_trade["strategy_amount"].iloc[i - 1]
        close_price = asset_trade["close_price"].iloc[i]
        trade_value = asset_trade["trade_value"].iloc[i]

        if (asset_trade["type"].iloc[i] == 1) and (trade_value > 0):
            strategy_name = asset_trade["strategy"].iloc[i]
            tmp_buy = buy(
                date=date_,
                current_price=close_price,
                balance=balance,
                available_value=math.floor(trade_value * max_share_of_market_trade),
                asset_type=asset_type,
                strategy=strategy_name
            )
            entry_price = close_price
            asset_trade["strategy_cash"].iloc[i] = tmp_buy["balance"]
            asset_trade["strategy_amount"].iloc[i] = tmp_buy["volume"]
            trade_table = trade_table + [tmp_buy]
            strategy_status = None

        elif (asset_trade["type"].iloc[i] == 2) and (amount > 0):
            strategy_status = 2
            strategy_name = asset_trade["strategy"].iloc[i]
            tmp_sell = sell(
                date=date_,
                entry_price=entry_price,
                balance=balance,
                current_price=close_price,
                volume=amount,
                available_value=math.floor(trade_value * max_share_of_market_trade),
                asset_type=asset_type,
                strategy=strategy_name
            )
            asset_trade["strategy_cash"].iloc[i] = tmp_sell["balance"]
            asset_trade["strategy_amount"].iloc[i] = amount - tmp_sell["volume"]
            trade_table = trade_table + [tmp_sell]
            if asset_trade["strategy_amount"].iloc[i] == 0:
                entry_price = None
                strategy_status = None
                strategy_name = None

        elif (strategy_status == 2) and (amount > 0):
            tmp_sell = sell(
                date=date_,
                entry_price=entry_price,
                balance=balance,
                current_price=close_price,
                volume=amount,
                available_value=math.floor(trade_value * max_share_of_market_trade),
                asset_type=asset_type,
                strategy=strategy_name
            )
            asset_trade["strategy_cash"].iloc[i] = tmp_sell["balance"]
            asset_trade["strategy_amount"].iloc[i] = amount - tmp_sell["volume"]
            trade_table = trade_table + [tmp_sell]
            if asset_trade["strategy_amount"].iloc[i] == 0:
                entry_price = None
                strategy_status = None
                strategy_name = None

        else:
            if buy_fix_income:
                asset_trade["strategy_cash"].iloc[i] = math.floor(asset_trade["strategy_cash"].iloc[i - 1] * (1 + (0.05/100)))
                asset_trade["strategy_amount"].iloc[i] = asset_trade["strategy_amount"].iloc[i - 1]
            else:
                asset_trade["strategy_cash"].iloc[i] = asset_trade["strategy_cash"].iloc[i - 1]
                asset_trade["strategy_amount"].iloc[i] = asset_trade["strategy_amount"].iloc[i - 1]


    strategy_balance = asset_trade["strategy_cash"].iloc[-1] + (
            asset_trade["strategy_amount"].iloc[-1] * asset_trade["close_price"].iloc[-1])

    return {
        "b&h_balance": int(asset_trade["b&h_balance"].iloc[-1]),
        "strategy_balance": int(strategy_balance),
        "trade_table": trade_table,
        "asset_signal": signal_asset_df,
        "asset_trade": asset_trade,
        "signals": signals_df
    }


if __name__ == "__main__":

    start_date = 1400_01_01
    account_balance = 10_000_000_000
    trade_share_of_market = 10 / 100 # maximum tradable volume is 10% of total traded volume of the day
    total_index_id = "32097828799138957"
    equal_index_id = "67130298613737946"
    top_30_index_id = "10523825119011581"
    metal_index_id = "32453344048876642"
    petrol_index_id = "12331083953323969"
    chemical_index_id = "33626672012415176"
    funds_symbol_id = ["17914401175772326", "33887145736684266",
                       "64942549055019553", "11427939669935844", "69067576215760005"]
    symbols = get_symbols_data()

    # signal_id = total_index_id
    # file_name = "total_index"
    # funds = symbols[symbols["symbol_id"].isin(funds_symbol_id)]
    # top30_stocks = get_index_symbols(index_id=top_30_index_id)
    # top30_stocks = symbols[symbols["symbol_id"].isin(top30_stocks["symbol_id"].values.tolist())]
    # symbols = pd.concat([top30_stocks, funds], axis=0, ignore_index=True)

    # signal_id, file_name = equal_index_id, "equal_index"
    # symbols = symbols[symbols["sector"] != "68"].sort_values(by="market_cap", ascending=False, ignore_index=True)
    # symbols = symbols.iloc[50:85].reset_index(drop=True)

    # signal_id, file_name = metal_index_id, "metal_index"
    # metal_stocks = get_index_symbols(index_id=metal_index_id)
    # symbols = symbols[symbols["symbol_id"].isin(metal_stocks["symbol_id"].values.tolist())]

    # signal_id, file_name = petrol_index_id, "petrol_index"
    # petrol_stocks = get_index_symbols(index_id=petrol_index_id)
    # symbols = symbols[symbols["symbol_id"].isin(petrol_stocks["symbol_id"].values.tolist())]

    signal_id, file_name = chemical_index_id, "chemical_index"
    chemical_stocks = get_index_symbols(index_id=chemical_index_id)
    symbols = symbols[symbols["symbol_id"].isin(chemical_stocks["symbol_id"].values.tolist())]

    results_stock_signal = []
    results_total_index_signal = []

    for c in tqdm.tqdm(range(len(symbols))):

        file_path = (f"c:/users/h.damavandi/desktop/strategy_backtest/{file_name}/"
                     f"trade limit {int(100 * trade_share_of_market)} prc/")

        symbol = symbols["symbol"].iloc[c]
        symbol_id = symbols["symbol_id"].iloc[c]
        symbol_type = symbols["yval"].iloc[c]

        returns_stock_signal = get_strategy_return(
            initial_balance=account_balance, from_date=start_date, asset_id_signal=symbol_id, asset_id_trade=symbol_id,
            asset_type=symbol_type, buy_fix_income=True, max_share_of_market_trade=trade_share_of_market)
        stock_signal_trades = pd.DataFrame(returns_stock_signal.pop("trade_table"))

        pathlib.Path(f"{file_path}/trades/stock_signal/").mkdir(parents=True, exist_ok=True)
        stock_signal_trades.to_excel(f"{file_path}/trades/stock_signal/{symbol}.xlsx", index=False)

        returns_stock_signal.pop("asset_signal")
        returns_stock_signal.pop("asset_trade")
        returns_stock_signal.pop("signals")
        returns_stock_signal = {"symbol": symbol, **returns_stock_signal}
        results_stock_signal.append(returns_stock_signal)


        returns_total_index_signal = get_strategy_return(
            initial_balance=account_balance, from_date=start_date, asset_id_signal=signal_id, asset_id_trade=symbol_id,
            asset_type=symbol_type, buy_fix_income=True, max_share_of_market_trade=trade_share_of_market)
        total_index_signal_trades = pd.DataFrame(returns_total_index_signal.pop("trade_table"))

        pathlib.Path(f"{file_path}/trades/{file_name}_signal/").mkdir(parents=True, exist_ok=True)
        total_index_signal_trades.to_excel(f"{file_path}/trades/{file_name}_signal/{symbol}.xlsx", index=False)

        returns_total_index_signal.pop("asset_signal")
        returns_total_index_signal.pop("asset_trade")
        returns_total_index_signal.pop("signals")
        returns_total_index_signal = {"symbol": symbol, **returns_total_index_signal}
        results_total_index_signal.append(returns_total_index_signal)


    results_stock_signal_df = pd.DataFrame(results_stock_signal)
    results_stock_signal_df["b&h_return"] = (results_stock_signal_df["b&h_balance"] / account_balance) - 1
    results_stock_signal_df["strategy_return"] = (results_stock_signal_df["strategy_balance"] / account_balance) - 1

    results_total_index_signal_df = pd.DataFrame(results_total_index_signal)
    results_total_index_signal_df["b&h_return"] = (results_total_index_signal_df["b&h_balance"] / account_balance) - 1
    results_total_index_signal_df["strategy_return"] = (results_total_index_signal_df["strategy_balance"] /
                                                        account_balance) - 1

    results_df = results_stock_signal_df.merge(
        results_total_index_signal_df, on="symbol", how="outer", suffixes=("_stock", f"_{file_name}"))

    pathlib.Path(f"{file_path}/").mkdir(parents=True, exist_ok=True)
    results_df.to_excel(f"{file_path}/results_df.xlsx", index=False)

