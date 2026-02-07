import yfinance as yf
import backtrader as bt


# data_ = yf.download('AAPL', start='2024-01-01', end='2025-01-29', interval='1h', multi_level_index=False)
data__ = yf.download("GC=F", start='2024-01-01', end='2025-01-29', interval='1h', multi_level_index=False)


####################################################################################################

# Strategy Configuration

class EMAStrategy(bt.Strategy):
    params = (
        ('ema_period', 20),  # EMA period
        ('risk_per_trade', 0.005),  # Risk 0.5% of Capital
        ('rr_ratio', 2),  # Risk/Reward ration of 1:2
        ('trail_distance', 5),  # Trailing distance of 5 pip
        ('min_candles_above', 5),  # Min of 5 Candles above EMA
    )

    def __init__(self):
        # EMA based on High and Low
        self.ema_high = bt.indicators.EMA(self.data.high, period=self.p.ema_period)
        self.ema_low = bt.indicators.EMA(self.data.low, period=self.p.ema_period)
        self.candles_above = 0  # Counter of Candles above the EMA

    def next(self):
        # Entry and Exit
        if not self.position:
            self.enter_trade()
        else:
            self.exit_trade()

    def enter_trade(self):
        # Entry Conditions
        if self.check_entry_conditions():
            size = self.calculate_position_size()
            if self.is_buy_signal():
                self.buy(size=size)
                self.stop_loss = self.data.low[-1]  # Stop Loss for Buy position
                self.take_profit = self.data.close[0] + (self.data.close[0] - self.stop_loss) * self.p.rr_ratio  # TP 1:2
            elif self.is_sell_signal():
                self.sell(size=size)
                self.stop_loss = self.data.high[-1]  # Stop Loss for Sell position
                self.take_profit = self.data.close[0] - (self.stop_loss - self.data.close[0]) * self.p.rr_ratio  # TP 1:2

    def exit_trade(self):
        # Exit Condirions
        if self.check_exit_conditions():
            self.close()

    def check_entry_conditions(self):
        # بررسی شرایط ورود (۵ کندل بالای EMA و برگشت به محدوده)
        if self.data.close[0] > self.ema_high[0] and self.data.close[0] > self.ema_low[0]:
            self.candles_above += 1
        else:
            self.candles_above = 0

        if self.candles_above >= self.p.min_candles_above and self.ema_low[0] < self.data.close[0] < self.ema_high[0]:
            return True
        return False

    def calculate_position_size(self):
        # محاسبه حجم معامله بر اساس ریسک ۰.۵٪
        risk_amount = self.broker.getvalue() * self.p.risk_per_trade
        if self.is_buy_signal():
            stop_loss_distance = self.data.close[0] - self.data.low[-1]
        else:
            stop_loss_distance = self.data.high[-1] - self.data.close[0]
        return risk_amount / stop_loss_distance


    def is_buy_signal(self):
        # بررسی سیگنال خرید (کندل برگشتی در تایم‌فریم ۱ ساعته)
        if (self.data.close[0] > self.data.open[0]):
                # and self.data.low[0] < self.data.low[-1]):
            return True
        return False

    def is_sell_signal(self):
        # بررسی سیگنال فروش (کندل برگشتی در تایم‌فریم ۱ ساعته)
        if self.data.close[0] < self.data.open[0]:
            # and self.data.high[0] > self.data.high[-1]:
            return True
        return False

    def check_exit_conditions(self):
        # بررسی شرایط خروج (حد سود، حد ضرر، تریلینگ استاپ)
        if self.position.size > 0:  # معامله خرید
            if self.data.close[0] >= self.take_profit:
                return True
            if self.data.close[0] <= self.stop_loss:
                return True
        elif self.position.size < 0:  # معامله فروش
            if self.data.close[0] <= self.take_profit:
                return True
            if self.data.close[0] >= self.stop_loss:
                return True
        return False

# تنظیمات بک‌تست
if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # افزودن استراتژی
    cerebro.addstrategy(EMAStrategy)

    # افزودن داده‌های تاریخی XAUUSD
    data = bt.feeds.PandasData(dataname=data__)
    cerebro.adddata(data)

    # تنظیم سرمایه اولیه
    cerebro.broker.set_cash(10000)

    # اجرای بک‌تست
    cerebro.run()

    # رسم نمودارها
    cerebro.plot()
