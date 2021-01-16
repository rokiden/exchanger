import typing
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class _SelfParsing:
    @classmethod
    def from_dict(cls, d: dict):
        o = cls()
        types = typing.get_type_hints(cls)
        for n, t in types.items():
            if n == 'ts':
                setattr(o, n, datetime.now())
            elif n in d:
                v = d[n]
                if ta := typing.get_args(t):
                    t = ta[0]
                if t == datetime:
                    try:
                        vt = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%fZ')
                    except:
                        vt = datetime.strptime(v, '%Y-%m-%dT%H:%M:%SZ')
                else:
                    vt = t(v)
                setattr(o, n, vt)
            else:
                setattr(o, n, None)

        return o


@dataclass(init=False)
class Ticker(_SelfParsing):
    symbol: str
    lastTradeRate: float
    bidRate: float
    askRate: float
    ts: datetime


@dataclass(init=False)
class Order(_SelfParsing):
    id: str
    marketSymbol: str
    direction: str
    type: str
    quantity: Optional[float]
    limit: Optional[float]
    ceiling: Optional[float]
    timeInForce: str
    fillQuantity: float
    commission: float
    proceeds: float
    status: str
    createdAt: datetime
    updatedAt: Optional[datetime]
    closedAt: Optional[datetime]


@dataclass(init=False)
class Balance(_SelfParsing):
    currencySymbol: str
    total: float
    available: float
    updatedAt: datetime


@dataclass(init=False)
class Trade(_SelfParsing):
    marketSymbol: Optional[str]
    id: str
    executedAt: datetime
    quantity: float
    rate: float
    takerSide: str


@dataclass(init=False)
class Execution(_SelfParsing):
    id: str
    marketSymbol: str
    executedAt: datetime
    quantity: float
    rate: float
    orderId: str
    commission: float
    isTaker: bool
