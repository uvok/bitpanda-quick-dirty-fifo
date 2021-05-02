from config import APIKEY

from decimal import Decimal as D
from collections import deque
from typing import List

import sqlalchemy.types as types

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base, relationship
import requests

import rfc3339

class SqliteNumeric(types.TypeDecorator):
    """
    Custom type for storing Decimals as string in SQLite.
    Otherwise SQLAlchemy complains about rounding
    """
    impl = types.String
    def process_bind_param(self, value, dialect):
        return str(value)
    def process_result_value(self, value, dialect):
        if value:
            return D(value)
        else:
            return D()

class RfcTimestamp(types.TypeDecorator):
    """
    Save date/time as string in SQLite.
    This way, I can handle formatting and timezone-awareness.
    """
    impl = types.String

    def process_bind_param(self, value, dialect):
        if type(value) is str:
            value = rfc3339.parse_datetime(value)

        if value:
            return value.isoformat()
        else:
            return None

    def process_result_value(self, value, dialect):
        return rfc3339.parse_datetime(value)

engine = sa.create_engine("sqlite:///transactions.db", echo=True, future=True)
Session = sessionmaker(engine)
Base = declarative_base()
#for debug only
resp={}

####################################################################################
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# When importing Bitpanda trades from the API, simply do a
# INSERT OR IGNORE so I don't have to worry about duplicates
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with('OR IGNORE'), **kw)
####################################################################################

class FiatTransfer(Base):
    """
    This table is for fiat transfers into the Bitpanda wallet
    Only one currency supported
    """

    __tablename__ = "fiat_transfers"
    id                  = sa.Column(sa.Integer, primary_key=True)
    amount              = sa.Column(SqliteNumeric)
    timestamp           = sa.Column(RfcTimestamp)

class Trade(Base):
    """
    This is a trade / exchange.
    It is specifically crafted to Bitpanda.
    """
    __tablename__ = "trades"

    id                  = sa.Column(sa.String, primary_key=True)
    trade_pair          = sa.Column(sa.String)
    # BUY, SELL
    transaction_type    = sa.Column(sa.String)

    amount              = sa.Column(SqliteNumeric)
    price               = sa.Column(SqliteNumeric)

    timestamp           = sa.Column(RfcTimestamp)

    is_best_fee         = sa.Column(sa.Boolean)
    fee                 = sa.Column(SqliteNumeric)
    fee_currency        = sa.Column(sa.String)

    @property
    def is_sale(self):
        return self.transaction_type == "SELL"

    @property
    def balance_left(self):
        """
        Balance change of the left side, usually Crypto.
        Positive if BUY, negative if SELL
        """
        if self.is_sale:
            value = -1 * self.amount
        else:
            value = self.amount

        # in case of "buy", the fee is on the crypto
        if not self.is_sale and not self.is_best_fee:
            value -= self.fee

        return value

    @property
    def balance_right(self):
        """
        Balance change of the right side, usually Fiat.
        Negtive if BUY, positive if SELL
        """
        if self.is_sale:
            value = self.amount * self.price
        else:
            value = -1 * self.amount * self.price

        # in case of "sell", the fee is on the fiat
        if self.is_sale and not self.is_best_fee:
            value -= self.fee

        return value

    @property
    def currency(self):
        return self.trade_pair.split("_")[0]

    def __repr__(self):
        return f"Trade(id={self.id[:6]}..., trade_pair={self.trade_pair!r}, transaction_type={self.transaction_type!r}, amount={self.amount!r}, price={self.price!r})"

Base.metadata.create_all(engine)

def import_trades():
    """
    Import trades from Bitpanda API
    """
    global resp

    with Session() as session:
        result = session.execute(
            sa.select(Trade).order_by(sa.desc("timestamp"))
        )
        latest = result.scalars().first()

    cursor=None
    s=requests.session()
    s.headers.update({'User-Agent': 'PyBitPandaFetcher'})
    s.headers.update({"Authorization": "Bearer "+APIKEY})

    alltrades=[]
    ppppage=1
    while True:
        ppppage += 1
        print(f"Fetching page {ppppage}")
        url='https://api.exchange.bitpanda.com/public/v1/account/trades'
        p={"max_page_size": 30}
        if latest:
            p["from"]=latest.timestamp.isoformat()
            p["to"]=rfc3339.now().isoformat()
        if cursor:
            p["cursor"] = cursor
        resp=s.get(url, params=p)
        if resp.status_code != 200:
            raise ValueError("Invalid status code")

        j=resp.json()

        trades = j["trade_history"]
        for trade in trades:
            t=trade["trade"]

            ormtrade=Trade(
                id=t["trade_id"],
                trade_pair=t["instrument_code"],
                transaction_type=t["side"], #BUY, SELL
                amount=D(t["amount"]),
                price=D(t["price"]),
                timestamp=rfc3339.parse_datetime(t["time"])
            )

            (tradee, traded) = t["instrument_code"].split("_")
            # switch currencies
            if t["side"] == "SELL":
                (tradee, traded) = (traded, tradee)

            f=trade["fee"]
            if f["collection_type"] == "BEST":
                ormtrade.is_best_fee = True
                ormtrade.fee = D(f["fee_amount"])
                ormtrade.fee_currency = f["fee_currency"]
            elif f["collection_type"] == "STANDARD":
                ormtrade.is_best_fee = False
                #fee_amount, fee_currency
                if f["fee_currency"] != tradee:
                    raise ValueError("Something appears to be wrong with the fee")
                ormtrade.fee = D(f["fee_amount"])
                ormtrade.fee_currency = f["fee_currency"]
            else:
                raise ValueError("Unknown fee collection type")

            alltrades.append(ormtrade)

        if not "cursor" in j:
            break
        else:
            cursor=j["cursor"]

    with Session() as session:
        session.add_all(alltrades)
        session.commit()
        pass

def get_all_trades() -> List[Trade]:
    """
    Get all trades, returning a list
    """
    with Session() as session:
        result = session.execute(
            sa.select(Trade).order_by("timestamp")
        )
        alltrades = result.scalars().all()
        return alltrades

def get_all_fiat():
    """
    Get all fiat deposits/withdrawals
    """
    with Session() as session:
        result = session.execute(
            sa.select(FiatTransfer).order_by("timestamp")
        )
        allfiat = result.scalars().all()
        return allfiat

from sqlalchemy import func

def get_bestfee_total():
    """
    Simply sum all the BEST fees
    """
    with Session() as session:
        result = session.execute(sa.select(func.sum(Trade.fee)).where(Trade.is_best_fee==True))
        return result.scalars().one()

def get_current_balances():
    """
    Get the current balances of each coin
    """
    trades=get_all_transactions()
    currencies={}
    for t in trades:
        if t.is_sale and not t.trade_pair in currencies:
            print(f"Ignoring Sale of {t.amount} {t.trade_pair.split('_')[0]}")
        elif not t.is_sale:
            currencies[t.trade_pair] = \
                currencies.get(t.trade_pair, 0) + t.balance_left
        elif t.is_sale:
            currencies[t.trade_pair] += t.balance_left
            if currencies[t.trade_pair] < 0:
                print("WARNING: Balance below 0 - how?")
        else:
            raise ValueError("Invalid condition")

    currencies["BEST_EUR"] = currencies.get("BEST_EUR", 0) - get_bestfee_total()

    return currencies

def calc_fifo():
    trades : List[Trade] = get_all_transactions()
    #trades = list(filter(lambda x: x.trade_pair == "PAN_EUR", trades))
    # dict containing dequeues for each currency
    fifo={}
    allgain = D()
    for t in trades:
        if t.is_sale:
            # no default value -  a BUY *MUST* be present!
            q : deque[Trade] = fifo.get(t.trade_pair)

            # invert sign to be positive, since this a sale
            t.remaining = -1 * t.balance_left
            assert(t.remaining == t.amount)

            while t.remaining > 0:
                matching_trade : Trade = q.popleft()

                if matching_trade.remaining >= t.remaining:
                    amnt = t.remaining
                    matching_trade.remaining -= t.remaining
                    t.remaining = 0

                    if matching_trade.remaining > 0:
                        # can be put back after subtracting
                        q.appendleft(matching_trade)
                else:
                    # not sufficient volume in current BUY transaction - fetch another one next loop
                    amnt = matching_trade.remaining
                    t.remaining -= matching_trade.remaining
                    matching_trade.remaining = 0

                buy = matching_trade.price * amnt
                sell = t.price * amnt
                # TODO
                fees = D()
                if t.is_sale and not t.is_best_fee:
                    fees = amnt / (-1 * t.balance_left) * t.fee

                print(f"CURRENCY: {t.currency}\tAMOUNT: {amnt:10.3f}\tBUY: {buy:10.3f} EUR\tSELL: {sell:10.3f} EUR\tFEE: {fees:10.3f} EUR\tGAIN: {(sell - buy - fees):10.3f} EUR")

                allgain += sell - buy - fees
            if t.remaining < 0:
                print("ERROR - balance shouldn't be below 0")
        else:
            if t.trade_pair not in fifo:
                fifo[t.trade_pair] = deque()

            q : deque = fifo.get(t.trade_pair)
            t.remaining = t.balance_left
            q.append(t)
    print(f"TOTAL GAIN: {allgain}")
