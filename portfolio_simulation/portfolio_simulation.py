from abc import ABC
from datetime import date
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Dict, Optional, Protocol, Tuple



# ---------- دارایی‌ها ----------
class AssetType(Enum):
    CASH_DIVIDEND = auto()
    BONUS_SHARES = auto()
    STOCK = auto()
    RIGHTS = auto()
    FUND = auto()
    BOND = auto()
    COMMODITY_CERT = auto()


class CAType(Enum):
    CASH_DIVIDEND = auto()
    BONUS_SHARES = auto()
    SPLIT = auto()
    COUPON = auto()  # برای اوراق
    RIGHTS_GRANT = auto()     # ← تخصیص حق‌تقدم به سهامدار موجود
    RIGHTS_EXERCISE = auto()  # ← اعمال بخشی/همهٔ حق‌تقدم و تبدیل به سهم پایه
    RIGHTS_LAPSE_SOLD = auto()  # ← جدید: فروش حق‌تقدم‌های استفاده‌نشده توسط ناشر
    CAPITAL_INCREASE = auto()   # ← کل فرایند افزایش سرمایه (جایزه + حق‌تقدم)
    DISTRIBUTION = auto()  # NEW: سود/توزیع نقدی برای سهم/صندوق



@dataclass(frozen=True)
class Asset(ABC):
    symbol: str
    asset_type: AssetType


class Stock(Asset):
    def __init__(self, symbol: str):
        super().__init__(symbol, AssetType.STOCK)
class Rights(Asset):
    def __init__(self, symbol: str, underlying_symbol: str, strike: float = 1000):
        super().__init__(symbol, AssetType.RIGHTS)
        object.__setattr__(self, "underlying_symbol", underlying_symbol)
        object.__setattr__(self, "strike", strike)
class Fund(Asset):
    def __init__(self, symbol: str):
        super().__init__(symbol, AssetType.FUND)
class Bond(Asset):
    def __init__(self, symbol: str):
        super().__init__(symbol, AssetType.BOND)
class CommodityCert(Asset):
    def __init__(self, symbol: str):
        super().__init__(symbol, AssetType.COMMODITY_CERT)


# ---------- قیمت ----------
class PriceProvider(Protocol):
    def get(self, d: date, asset: Asset) -> Optional[float]: ...

class DictPriceProvider:
    """prices[(date, symbol)] = price"""
    def __init__(self, prices: Dict[Tuple[date,str], float]):
        self.prices = prices
    def get(self, d: date, asset: Asset) -> Optional[float]:
        return self.prices.get((d, asset.symbol))


# -------- پوزیشن با «بهای تمام‌شدهٔ کل» --------
@dataclass
class Position:
    asset: Asset
    quantity: float = 0.0           # تعداد کل
    total_cost: float = 0.0         # بهای تمام‌شدهٔ کل (ریال)
    realized_pnl: float = 0.0       # سود/زیان تحقق‌یافته
    dividend_cash: float = 0.0      # سود نقدی دریافتی

    # قفل ساده در سطح پوزیشن (برای سناریوی سهام جایزه)
    locked_qty: float = 0.0
    unlock_date: Optional[date] = None

    @property
    def avg_cost(self) -> float:
        return (self.total_cost / self.quantity) if self.quantity > 0 else 0.0

    def is_unlocked(self, asof: date) -> bool:
        return (self.unlock_date is None) or (asof >= self.unlock_date) or (self.locked_qty <= 1e-12)

    def available_qty(self, asof: date) -> float:
        if self.is_unlocked(asof): return self.quantity
        return max(self.quantity - self.locked_qty, 0.0)

    # خرید/فروش
    def buy(self, qty: float, price: float, asof: date):
        if qty <= 0: return
        self.quantity += qty
        self.total_cost += qty * price
        # خرید معمولی قفل ندارد

    def sell(self, qty: float, price: float, asof: date):
        if qty <= 0: return
        if qty > self.available_qty(asof) + 1e-9:
            raise ValueError("Not enough UNLOCKED quantity to sell")
        ac = self.avg_cost
        self.realized_pnl += (price - ac) * qty
        self.total_cost -= ac * qty
        self.quantity -= qty
        # اگر مقداری از locked باقی مانده، بدون تغییر می‌ماند
        if self.quantity <= 1e-12:
            self.quantity = 0.0
            self.total_cost = 0.0
            self.locked_qty = 0.0
            self.unlock_date = None

    # --- رویدادهای شرکتی ---
    def cash_dividend(self, per_share: float) -> float:
        amount = self.quantity * per_share
        self.dividend_cash += amount
        return amount  # تا پورتفو نقد را افزایش دهد

    def bonus_shares(self, ratio: float):
        """سهام جایزه: تعداد ↑ ، بهای تمام‌شدهٔ کل ثابت می‌ماند."""
        if ratio <= 0 or self.quantity <= 0: return
        self.quantity *= (1 + ratio)  # total_cost تغییر نمی‌کند

    # افزودن سهام جایزه (قفل تا تاریخ ثبت)
    def add_bonus_locked(self, qty: float, cost_alloc: float, lock_until: date):
        if qty <= 0: return
        self.quantity += qty
        self.total_cost += cost_alloc
        self.locked_qty += qty
        # اگر قبلاً unlock_date داشت، حداکثرِ آن دو تاریخ را نگه می‌داریم
        if (self.unlock_date is None) or (lock_until > self.unlock_date):
            self.unlock_date = lock_until

    # آزادسازی قفل اگر تاریخش رسیده باشد (اختیاری برای فراخوانی دوره‌ای)
    def refresh_lock(self, asof: date):
        if self.unlock_date and asof >= self.unlock_date:
            self.locked_qty = 0.0
            self.unlock_date = None

    def split(self, factor: float):
        if factor <= 0: raise ValueError("split factor must be > 0")
        if self.quantity <= 0: return
        self.quantity *= factor  # total_cost ثابت

    def rights_issue(self, rights_ratio: float, strike_price: float, exercise: bool) -> float:
        """
        rights_ratio: تعداد حق‌تقدم به ازای هر سهم موجود (بر مبنای قبل از اعمال)
        در صورت exercise=True، خرید انجام می‌شود: total_cost افزایش می‌یابد.
        برمی‌گرداند: مبلغ نقدی لازم برای اعمال (برای ثبت در پورتفو).
        """
        if rights_ratio <= 0 or self.quantity <= 0: return 0.0
        rights_qty = self.quantity * rights_ratio
        if exercise and rights_qty > 0:
            self.quantity += rights_qty
            cash_needed = rights_qty * strike_price
            self.total_cost += cash_needed
            return cash_needed
        return 0.0

    def distribution_amount(self, per_unit: float) -> float:
        """مبلغ کل سود روی این پوزیشن (بدون اعمال در نقد)."""
        if per_unit <= 0 or self.quantity <= 0: return 0.0
        return self.quantity * per_unit


# ---------- تراکنش‌ها/رویدادها ----------
class TxType(Enum):
    BUY = auto()
    SELL = auto()
    CASH_IN = auto()
    CASH_OUT = auto()
    FEE = auto()
    TAX = auto()


@dataclass
class Transaction:
    d: date
    ttype: TxType
    asset: Optional[Asset] = None
    qty: float = 0.0
    price: float = 0.0
    amount: float = 0.0   # برای CASH_IN/OUT/FEE/TAX


@dataclass
class CapitalIncreaseParams:
    # ورودی‌های شرکت/بازار
    old_share: float          # تعداد کل سهام شرکت قبل از افزایش سرمایه
    price_pre: float          # قیمت سهم قبل از افزایش سرمایه (برای محاسبه قیمت تعدیل‌شده)
    contribution: float       # تعداد سهام ناشی از آورده نقدی
    premium: float            # تعداد سهام ناشی از سلب حق‌تقدم (به ما تعلق نمی‌گیرد)
    reserve: float            # تعداد سهام ناشی از اندوخته (سهام جایزه)
    record_unlock_date: date  # تاریخ ثبت/آزادشدن سهام جایزه


@dataclass
class CorporateAction:
    d: date
    catype: CAType
    equity: Optional[Stock] = None
    rights: Optional[Rights] = None

    fund:   Optional[Fund]   = None
    # برای DISTRIBUTION:
    per_unit: float = 0.0           # مبلغ سود به ازای هر واحد
    reinvest: bool = False          # سرمایه‌گذاری مجدد؟
    reinvest_price: float = 0.0     # قیمت خرید برای DRIP (NAV/قیمت روز)

    # برای CAPITAL_INCREASE
    capi: Optional[CapitalIncreaseParams] = None
    # پارامترهای عمومی
    dividend_per_share: float = 0.0
    bonus_ratio: float = 0.0
    split_factor: float = 1.0
    # حقوق
    rights_ratio: float = 0.0
    # برای RIGHTS_EXERCISE / RIGHTS_LAPSE_SOLD
    exercise_qty: float = 0.0
    # پارامترهای جدید برای Lapse+Sale
    lapse_qty: float = 0.0
    sale_price: float = 0.0         # قیمت هر حق‌تقدم (اختیاری)
    sale_fee: float = 0.0           # کارمزد/هزینه حراج (اختیاری)
    cash_proceeds: float = 0.0      # اگر عواید نهایی مستقیم اعلام شود


# --- تابعی که از کدت استفاده می‌کند (بدون تغییر منطق تو) ---
def compute_allocation(amount: float, total_cost: float, price: float,
                       old_share: float, contribution: float, premium: float, reserve: float):
    # از روی تصویرت بازنویسی شده
    bonus = (amount * (reserve / old_share)) // 1
    ros   = (amount * (contribution / old_share)) // 1
    cost  = (total_cost + (1000 * ros)) / (amount + bonus + ros)
    cost_share = cost * amount
    cost_bonus = cost * bonus
    cost_ros   = (cost - 1000) * ros
    new_share = old_share + contribution + premium + reserve
    old_cap   = old_share * price
    if premium == 0:
        adj_price = ((old_cap + (1000 * contribution)) / new_share) // 1
    else:
        adj_price = ((old_cap + (1000 * (contribution + premium + reserve))) / new_share) // 1
    return {
        "adj_price": adj_price,
        "ros_price": adj_price - 1000,
        "bonus": bonus,
        "ros": ros,
        "cost_share": cost_share,
        "cost_bonus": cost_bonus,
        "cost_ros": cost_ros,
    }


# ---------- پرتفوی ----------
@dataclass
class Portfolio:
    cash: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    ledger: list[tuple[date, str, float]] = field(default_factory=list)

    def _pos(self, asset: Asset) -> Position:
        p = self.positions.get(asset.symbol)
        if p is None:
            p = Position(asset)
            self.positions[asset.symbol] = p
        return p

    def _find_equity(self, symbol: str) -> Optional[Position]:
        p = self.positions.get(symbol)
        return p if p and isinstance(p.asset, Stock) else None

    def apply_transaction(self, tx: Transaction):
        if tx.ttype == TxType.BUY:
            assert tx.asset is not None
            self._pos(tx.asset).buy(tx.qty, tx.price, tx.d)
            self.cash -= tx.qty * tx.price
            self.ledger.append((tx.d, f"BUY {tx.asset.symbol} x{tx.qty}@{tx.price}", -tx.qty * tx.price))
        elif tx.ttype == TxType.SELL:
            assert tx.asset is not None
            self._pos(tx.asset).sell(tx.qty, tx.price, tx.d)
            self.cash += tx.qty * tx.price
            self.ledger.append((tx.d, f"SELL {tx.asset.symbol} x{tx.qty}@{tx.price}", tx.qty * tx.price))
        elif tx.ttype == TxType.CASH_IN:
            self.cash += tx.amount
            self.ledger.append((tx.d, "CASH IN", tx.amount))
        elif tx.ttype == TxType.CASH_OUT:
            self.cash -= tx.amount
            self.ledger.append((tx.d, "CASH OUT", -tx.amount))
        elif tx.ttype == TxType.FEE:
            self.cash -= tx.amount
            self.ledger.append((tx.d, "FEE", -tx.amount))
        elif tx.ttype == TxType.TAX:
            self.cash -= tx.amount
            self.ledger.append((tx.d, "TAX", -tx.amount))
        else:
            raise ValueError("Unknown transaction type")


    def apply_corporate_action(self, ca: CorporateAction):

        if ca.catype == CAType.CAPITAL_INCREASE:
            assert ca.equity is not None and ca.capi is not None
            ep = self._pos(ca.equity)
            # قبل از تخصیص، قفل‌ها را به‌روز کنیم (اگر تاریخ ثبت گذشته)
            ep.refresh_lock(ca.d)

            # مقدار «سهم ما» در تاریخ رکورد
            amount = ep.quantity
            alloc = compute_allocation(
                amount=amount,
                total_cost=ep.total_cost,
                price=ca.capi.price_pre,
                old_share=ca.capi.old_share,
                contribution=ca.capi.contribution,
                premium=ca.capi.premium,
                reserve=ca.capi.reserve,
            )
            bonus = alloc["bonus"]
            ros   = alloc["ros"]

            # 1) بهای تمام‌شده‌ی سهم پایه ← cost_share
            old_total = ep.total_cost
            ep.total_cost = alloc["cost_share"] + alloc["cost_bonus"]  # سهم موجود + جایزه
            # 2) تعداد سهم پایه + سهام جایزه (جایزه قفل می‌شود تا تاریخ ثبت)
            if bonus > 0:
                # ما cost_bonus را هم به پوزیشن اضافه کردیم (بالا)؛ اینجا فقط قفل و تعداد را مدیریت می‌کنیم
                ep.add_bonus_locked(qty=bonus, cost_alloc=0.0, lock_until=ca.capi.record_unlock_date)
            # (توجه: cost_bonus قبلاً در ep.total_cost لحاظ شده است)

            # 3) حق‌تقدم (دارایی مستقل + قابل معامله)
            if ros > 0:
                # از بهای دفتری سهم پایه به اندازه cost_ros منتقل شده (در سطر بالا با جایگزینی total_cost رعایت شد)
                # حالا پوزیشن حق‌تقدم را به‌روز کنیم:
                assert ca.rights is not None, "rights asset required"
                rp = self._pos(ca.rights)
                rp.quantity   += ros
                rp.total_cost += alloc["cost_ros"]
                self.ledger.append((ca.d, f"CAPITAL INCREASE: bonus {bonus}, rights {ros}", 0.0))

        elif ca.catype == CAType.CASH_DIVIDEND:
            assert ca.equity is not None
            pos = self._pos(ca.equity)
            amount = pos.cash_dividend(ca.dividend_per_share)
            self.cash += amount
            self.ledger.append((ca.d, f"DIV {ca.equity.symbol}", amount))

        elif ca.catype == CAType.RIGHTS_LAPSE_SOLD:
            assert ca.rights is not None and ca.lapse_qty > 0
            rp = self._pos(ca.rights)
            if ca.lapse_qty > rp.quantity + 1e-9:
                raise ValueError("lapse qty > rights qty")
            # عواید نقدی
            if ca.cash_proceeds > 0:
                proceeds = ca.cash_proceeds
            else:
                proceeds = ca.lapse_qty * ca.sale_price - ca.sale_fee
            proceeds = max(proceeds, 0.0)  # از منفی شدن جلوگیری کنیم (در صورت ورود اشتباه)
            # بهای دفتریِ بخشِ واگذار‌شده (میانگین موزون)
            carrying = rp.avg_cost * ca.lapse_qty
            # تحقق سود/زیان در پوزیشن حق‌تقدم
            rp.realized_pnl += (proceeds - carrying)
            # کاهش پوزیشن حق‌تقدم
            rp.total_cost -= carrying
            rp.quantity   -= ca.lapse_qty
            if rp.quantity <= 1e-12:
                rp.quantity, rp.total_cost = 0.0, 0.0
            # افزایش نقد
            self.cash += proceeds
            self.ledger.append((ca.d, f"RIGHTS LAPSE-SOLD {ca.rights.symbol} x{ca.lapse_qty}", proceeds))


        elif ca.catype == CAType.BONUS_SHARES:
            assert ca.equity is not None
            pos = self._pos(ca.equity); pos.bonus_shares(ca.bonus_ratio)
            self.ledger.append((ca.d, f"BONUS {ca.equity.symbol} {ca.bonus_ratio*100:.2f}%", 0.0))

        elif ca.catype == CAType.SPLIT:
            assert ca.equity is not None
            pos = self._pos(ca.equity); pos.split(ca.split_factor)
            self.ledger.append((ca.d, f"SPLIT {ca.equity.symbol} x{ca.split_factor}", 0.0))

        elif ca.catype == CAType.RIGHTS_GRANT:
            # اعطای حق‌تقدم به نسبت دارایی موجود در سهم پایه
            assert ca.equity is not None and ca.rights is not None
            eq_pos = self._pos(ca.equity)
            rights_pos = self._pos(ca.rights)
            granted = eq_pos.quantity * ca.rights_ratio
            if granted > 0:
                rights_pos.quantity += granted
                # بهای تمام‌شدهٔ اعطایی صفر؛ اگر خواستی می‌توانی بخشی از بهای سهم پایه را تفکیک کنی (سیاست حسابداری)
                self.ledger.append((ca.d, f"RIGHTS GRANT {ca.rights.symbol} +{granted}", 0.0))

        # elif ca.catype == CAType.RIGHTS_EXERCISE:
        #     # اعمال بخشی از حق‌تقدم و تبدیل به سهم پایه
        #     assert ca.rights is not None
        #     rights_pos = self._pos(ca.rights)
        #     if ca.exercise_qty <= 0 or ca.exercise_qty > rights_pos.quantity + 1e-9:
        #         raise ValueError("Invalid exercise quantity")
        #     # هزینهٔ نسبتی منتقل‌شونده از پوزیشن حق‌تقدم
        #     rights_ac = rights_pos.avg_cost
        #     moved_cost = rights_ac * ca.exercise_qty
        #
        #     # کم‌کردن از حق‌تقدم
        #     rights_pos.total_cost -= moved_cost
        #     rights_pos.quantity -= ca.exercise_qty
        #     if rights_pos.quantity <= 1e-12:
        #         rights_pos.quantity, rights_pos.total_cost = 0.0, 0.0
        #
        #     # افزودن به سهم پایه
        #     # توجه: strike را از خود آبجکت Rights برمی‌داریم
        #     strike = ca.rights.strike
        #     # اگر سهم پایه هنوز در پورتفو نیست، بساز:
        #     eq_symbol = ca.rights.underlying_symbol
        #     eq_pos = self.positions.get(eq_symbol)
        #     if eq_pos is None:
        #         eq_pos = Position(Stock(eq_symbol))
        #         self.positions[eq_symbol] = eq_pos
        #
        #     eq_pos.quantity += ca.exercise_qty
        #     eq_pos.total_cost += moved_cost + (ca.exercise_qty * strike)
        #
        #     # وجه نقد پرداختی بابت اعمال
        #     cash_needed = ca.exercise_qty * strike
        #     self.cash -= cash_needed
        #     self.ledger.append((ca.d, f"RIGHTS EXERCISE {ca.rights.symbol}→{eq_symbol} x{ca.exercise_qty} @ {strike}", -cash_needed))

        elif ca.catype == CAType.RIGHTS_EXERCISE:
            assert ca.rights is not None and ca.exercise_qty > 0
            rp = self._pos(ca.rights)
            if ca.exercise_qty > rp.quantity + 1e-9:
                raise ValueError("exercise qty > rights qty")
            moved_cost = rp.avg_cost * ca.exercise_qty
            rp.total_cost -= moved_cost
            rp.quantity   -= ca.exercise_qty
            if rp.quantity <= 1e-12:
                rp.quantity, rp.total_cost = 0.0, 0.0
            # به سهم پایه منتقل شود
            eq_symbol = ca.rights.underlying_symbol
            ep = self.positions.get(eq_symbol) or self._pos(Stock(eq_symbol))
            ep.quantity   += ca.exercise_qty
            ep.total_cost += moved_cost + ca.rights.strike * ca.exercise_qty
            # وجه نقد پرداختی
            cash_needed = ca.rights.strike * ca.exercise_qty
            self.cash -= cash_needed
            self.ledger.append((ca.d, f"RIGHTS EXERCISE {ca.rights.symbol}->{eq_symbol} x{ca.exercise_qty}", -cash_needed))

        elif ca.catype == CAType.DISTRIBUTION:
            # دارایی می‌تواند سهم یا صندوق باشد:
            asset = ca.equity or ca.fund
            assert asset is not None, "asset (equity or fund) required"
            pos = self._pos(asset)

            gross = pos.distribution_amount(ca.per_unit)
            if gross <= 0:
                return

            if ca.reinvest:
                # با کل عایدی، واحد جدید می‌خریم
                if ca.reinvest_price <= 0:
                    raise ValueError("reinvest requires reinvest_price > 0")
                qty_buy = gross / ca.reinvest_price
                # خریدِ بدون قفل
                pos.buy(qty_buy, ca.reinvest_price, ca.d)
                # توجه: در DRIP نقد تغییر نمی‌کند (درآمد = هزینهٔ خرید)
                self.ledger.append(
                    (ca.d, f"DISTRIBUTION-DRIP {asset.symbol} +{qty_buy:.6f} @ {ca.reinvest_price}", 0.0))
            else:
                # واریز نقدی
                self.cash += gross
                self.ledger.append((ca.d, f"DISTRIBUTION {asset.symbol}", gross))
        else:
            raise ValueError("Unknown corporate action")


####################################################################################################

    # ارزش‌گذاری و سود/زیان
    def market_value(self, d: date, pricer: PriceProvider) -> float:
        total = self.cash
        for pos in self.positions.values():
            px = pricer.get(d, pos.asset)
            if px is not None:
                total += pos.quantity * px
        return total

    def unrealized_pnl(self, d: date, pricer: PriceProvider) -> float:
        pnl = 0.0
        for pos in self.positions.values():
            px = pricer.get(d, pos.asset)
            if px is not None and pos.quantity > 0:
                pnl += pos.quantity * (px - pos.avg_cost)
        return pnl

    def realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self.positions.values())

    def dividends_received(self) -> float:
        return sum(p.dividend_cash for p in self.positions.values())


# ---------- مثال استفاده ----------
if __name__ == "__main__":

    from datetime import date

    pf = Portfolio()
    tepix = Stock("TICKER1")
    pf.apply_transaction(Transaction(date(2025,1,1), TxType.CASH_IN, amount=100_000_000))
    pf.apply_transaction(Transaction(date(2025,1,2), TxType.BUY, asset=tepix, qty=1000, price=50_000))
    # سهام جایزه 20%
    pf.apply_corporate_action(CorporateAction(date(2025,5,15), tepix, CAType.BONUS_SHARES, bonus_ratio=0.2))
    # فروش 400 سهم @70k
    pf.apply_transaction(Transaction(date(2025,6,1), TxType.SELL, asset=tepix, qty=400, price=70_000))
    prices = {(date(2025,6,1), "TICKER1"): 70_000}
    mv = pf.market_value(date(2025,6,1), DictPriceProvider(prices))
    print("qty:", pf.positions["TICKER1"].quantity)
    print("avg_cost:", pf.positions["TICKER1"].avg_cost)
    print("realized PnL:", pf.realized_pnl())
    print("MV:", mv, "Unrealized:", pf.unrealized_pnl(date(2025,6,1), DictPriceProvider(prices)))


    pf = Portfolio()
    eq = Stock("XYZ")
    r  = Rights("XYZح", underlying_symbol="XYZ", strike=1000)
    # فرض: قبلاً 1000 سهم داریم و 300 حق‌تقدم تخصیص گرفته
    pf.apply_transaction(Transaction(date(2025,1,1), TxType.CASH_IN, amount=100_000_000))
    pf.apply_transaction(Transaction(date(2025,1,2), TxType.BUY, asset=eq, qty=1000, price=5000))
    pf.apply_corporate_action(CorporateAction(date(2025,3,1), CAType.RIGHTS_GRANT, equity=eq, rights=r, rights_ratio=0.3))
    # کمی هم حق‌تقدم از بازار می‌خریم تا avg>0 شود:
    pf.apply_transaction(Transaction(date(2025,3,5), TxType.BUY, asset=r, qty=100, price=200))
    # بخشی را اعمال می‌کنیم (مثلاً 150 عدد)
    pf.apply_corporate_action(CorporateAction(date(2025,3,20), CAType.RIGHTS_EXERCISE, rights=r, exercise_qty=150))
    # مهلت می‌گذرد؛ ناشر باقی مانده را در حراج می‌فروشد (مثلاً 230 عدد باقی‌مانده با قیمت 180، کارمزد 10,000)
    pf.apply_corporate_action(CorporateAction(
        date(2025,4,30), CAType.RIGHTS_LAPSE_SOLD, rights=r, lapse_qty=230, sale_price=180, sale_fee=10_000))
    print("Cash:", pf.cash)
    rp = pf.positions.get("XYZح")
    if rp: print("Rights qty/avg/realized:", rp.quantity, rp.avg_cost, rp.realized_pnl)
    ep = pf.positions.get("XYZ")
    print("Equity qty/avg:", ep.quantity, ep.avg_cost)


    etf = Fund("FUND1")
    # فرض: 1000 واحد صندوق داریم
    pf.apply_transaction(Transaction(date(2025,6,1), TxType.CASH_IN, amount=50_000_000))
    pf.apply_transaction(Transaction(date(2025,6,2), TxType.BUY, asset=etf, qty=1000, price=10_000))
    # توزیع سود نقدی 300 ریال به ازای هر واحد
    pf.apply_corporate_action(CorporateAction(date(2025,7,1), CAType.DISTRIBUTION, fund=etf, per_unit=300))
    # سناریوی DRIP: همان 300 ریال، اما سرمایه‌گذاری مجدد با NAV=9,800
    pf.apply_corporate_action(
        CorporateAction(
            date(2025,10,1), CAType.DISTRIBUTION, fund=etf, per_unit=300, reinvest=True, reinvest_price=9_800))





















