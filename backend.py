import asyncio
import logging
from typing import Callable, Union

from aiobittrexapi import Bittrex

import corotools
from bittrexsocket import BittrexSocket
from cacher import Cacher
from schema import *

log = logging.getLogger(__name__)


class Backend:
    def __init__(self, api_key: str, api_secret: str, markets: dict[str, list[str]],
                 callbacks: dict[str, Union[Callable, list[Callable]]]):
        """
        :param markets: {'ticker':['BTC-USD','ETH-USD'],'trade':['BTC-USD']}
        :param callbacks: {'ticker':on_ticker,'order':on_order,'balance':[on_balance,log_balance]}
        """
        self.api = Bittrex(api_key, api_secret)
        self.__api_ws = BittrexSocket(api_key, api_secret)
        self.__markets = markets
        self.__callbacks = callbacks
        self.__ctrl_event = asyncio.Event()
        self.__ctrl_stop = False

        self.__converters = {'ticker': Ticker.from_dict,
                             'trade': lambda d: [Trade.from_dict(t | d) for t in d['deltas']],
                             'order': lambda d: Order.from_dict(d['delta']),
                             'balance': lambda d: Balance.from_dict(d['delta']),
                             'execution': lambda d: [Execution.from_dict(e) for e in d['deltas']]}

    @Cacher(30)
    async def get_balances(self, skip_empty=True):
        res = await self.api.get_balances()
        balances = [Balance.from_dict(d) for d in res.values()]
        if skip_empty:
            balances = [b for b in balances if b.total != 0]
        return balances

    @Cacher(30)
    async def get_orders(self, opened: bool, closed: bool, limit):
        jobs = []
        if opened:
            jobs.append(self.api.get_open_orders())
        if closed:
            jobs.append(self.api.get_closed_orders())
        res = await asyncio.gather(*jobs)
        ords = [Order.from_dict(d) for j in res for d in j][:limit]
        return ords

    @Cacher(5)
    async def get_tickers(self):
        res = await self.api.get_tickers()
        return [Ticker.from_dict(d) for d in res.values()]

    async def run(self):
        while True:
            log.info(f'Starting')

            self.__ctrl_stop = False
            self.__ctrl_event.clear()

            channels = []
            callbacks = {}
            for n, c in self.__callbacks.items():
                if n in self.__markets:
                    channels.extend([n + '_' + m for m in self.__markets[n]])
                else:
                    channels.append(n)
                if isinstance(c, list):
                    c = corotools.wrapmulti(c)
                if n in self.__converters:
                    c = corotools.wrapfunc(c, self.__converters[n])
                callbacks[n] = c

            task_listen = asyncio.create_task(self.__api_ws.listen(channels, callbacks))
            task_ctrl = asyncio.create_task(self.__ctrl_event.wait())

            log.info(f'Running')

            done, pending = await asyncio.wait([task_ctrl, task_listen], return_when=asyncio.FIRST_COMPLETED)
            if task_listen in done:  # error in listen task
                log.error(f'Error in listen task')
                return
            else:
                log.info(f'Ctrl event, stop: {self.__ctrl_stop}')
                self.__api_ws.stop()
                await task_listen
                log.info(f'Listen task stopped')
                if self.__ctrl_stop:
                    return

    def stop(self):
        self.__ctrl_stop = True
        self.__ctrl_event.set()

    def restart(self):
        self.__ctrl_stop = False
        self.__ctrl_event.set()
