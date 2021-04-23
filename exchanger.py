import asyncio
import itertools
import logging
import os
import argparse

from dataclasses import dataclass
from typing import Optional

from aiogram import types
from configobj import ConfigObj
from pycbrf import ExchangeRates

import backend
import cacher
import schema
import tgbot


def __get_usd_rub_rate(date=None):
    try:
        return float(ExchangeRates(on_date=date)['USD'].rate)
    except Exception as e:
        print('ExchangeRates exc')
        return None


@cacher.Cacher(600, 1)
async def get_usd_rub_rate(date=None):
    return asyncio.get_event_loop().run_in_executor(None, __get_usd_rub_rate, date)


def format_float(f, symbols):
    return f'{f:.0{symbols - 2}f}'[:symbols]


ff = format_float


def format_order(o: schema.Order, maxlen=0):
    return f'`{o.marketSymbol:>{maxlen}}` \\[{ff(o.limit, 10)}] {ff(o.quantity, 8)} ' \
           f'({o.fillQuantity * 100 / o.quantity:.02f}%)'


def format_execution(e: schema.Execution, maxlen=0):
    return f'`{e.marketSymbol:>{maxlen}}` \\[{ff(e.rate, 10)}] {ff(e.quantity, 8)}'


def format_balance(b: schema.Balance, maxlen=0):
    return f'`{b.currencySymbol:>{maxlen}}` {ff(b.total, 10)} ' \
           f'({b.available * 100 / b.total:.02f}%)'


@dataclass
class _SummaryCoin:
    symbol: str
    value: Optional[float]
    inbtc: Optional[float]
    inusd: Optional[float]
    inrub: Optional[float]
    sum: bool = False


def format_summary(c: _SummaryCoin, maxlen):
    s = f'`{c.symbol:>{maxlen}}` '
    if c.value is not None:
        s += f'{ff(c.value, 10)}'
    if c.inbtc is not None:
        s += f'\n`{"B":>{maxlen}}` {ff(c.inbtc, 10)}'
    if c.inusd is not None:
        s += f'\n`{"$":>{maxlen}}` {c.inusd:.2f}'
    if c.inrub is not None:
        s += f'\n`{"₽":>{maxlen}}` {c.inrub:.2f}'
    return s


async def main():
    await bot.api.send_message(bot.user_id, 'Exchanger starting...')
    task_backend = asyncio.create_task(back.run())
    task_tgbot = asyncio.create_task(bot.polling())
    done, pending = await asyncio.wait([task_backend, task_tgbot], return_when=asyncio.FIRST_COMPLETED)
    if task_tgbot in pending:
        await bot.stop()
    if task_backend in pending:
        await back.stop()


async def cmd_balance(message: types.Message):
    balances = await back.get_balances()
    maxlen = max([len(b.currencySymbol) for b in balances])
    resp = '\n'.join([format_balance(b, maxlen) for b in balances])
    await message.answer(resp, parse_mode='markdown')


async def cmd_orders(message: types.Message):
    arg = message.get_full_command()[1]
    limit = 10 if len(arg) == 0 else int(arg)
    os = await back.get_orders(True, True, limit)
    maxlen = max([len(o.marketSymbol) for o in os])

    resp = ''
    for status, os_st in itertools.groupby(os, lambda o: o.status):
        os_st = list(os_st)
        resp += f'==== {status} [{len(os_st)}] ====\n'
        for dir, os_dir in itertools.groupby(sorted(os_st, key=lambda o: o.direction), key=lambda o: o.direction):
            os_dir = list(os_dir)
            resp += f'== {dir} [{len(os_dir)}] ==\n'
            for o in os_dir:
                resp += format_order(o, maxlen=maxlen) + '\n'
    await message.answer(resp, parse_mode='markdown')


async def cmd_summary(message: types.Message):
    balances, tickers, usd_rub = await asyncio.gather(back.get_balances(), back.get_tickers(), get_usd_rub_rate())
    tickers_dict = {t.symbol: t for t in tickers}

    usd_rub = usd_rub.result()
    btc_usd = tickers_dict['BTC-USD'].lastTradeRate

    coins: list[_SummaryCoin] = []

    for b in balances:
        if (market := f'{b.currencySymbol}-BTC') in tickers_dict:
            coins.append(
                _SummaryCoin(b.currencySymbol, b.total, tickers_dict[market].lastTradeRate * b.total, None, None))
        elif (market := f'{b.currencySymbol}-USD') in tickers_dict:
            coins.append(
                _SummaryCoin(b.currencySymbol, None, b.total, tickers_dict[market].lastTradeRate * b.total, None))
    coin_alts = _SummaryCoin('alts', None,
                             sum([c.inbtc for c in coins if c.value is not None and c.inbtc is not None]),
                             None, None, sum=True)
    coins.append(coin_alts)
    for c in coins:
        if c.inusd is None:
            c.inusd = c.inbtc * btc_usd
    if usd_rub is not None:
        for c in coins:
            c.inrub = c.inusd * usd_rub

    coin_total = _SummaryCoin('total', None,
                              sum([c.inbtc for c in coins if c.inbtc is not None and not c.sum]),
                              sum([c.inusd for c in coins if c.inusd is not None and not c.sum]),
                              sum([c.inrub for c in coins if c.inrub is not None and not c.sum]),
                              sum=True)
    coins.append(coin_total)

    maxlen = max([len(c.symbol) for c in coins])
    resp = '\n'.join([format_summary(c, maxlen) for c in coins])
    await message.answer(resp, parse_mode='markdown')


async def on_private(arg):
    print('on_private', arg)
    if isinstance(arg, schema.Balance):
        msg = format_balance(arg)
    elif isinstance(arg, schema.Order):
        msg = f'{arg.status} {arg.direction}\n' \
              + format_order(arg)
    elif isinstance(arg, list) and isinstance(arg[0], schema.Execution):
        msg = '\n'.join([format_execution(e) for e in arg])
    else:
        msg = str(arg)
    await bot.api.send_message(bot.user_id, msg, parse_mode='markdown')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=open, default='exchanger.ini')
    parser.add_argument('--log-level', default='INFO',
                        choices=[logging.getLevelName(l) for l in (logging.DEBUG, logging.INFO, logging.WARN)])

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    config = ConfigObj(args.config)
    back = backend.Backend(config['bx_key'], config['bx_secret'], {},
                           {'balance': on_private, 'order': on_private, 'execution': on_private})
    bot = tgbot.TGBot(config['tg_token'], config['tg_user_id'],
                      {'balance': cmd_balance, 'orders': cmd_orders, 'summary': cmd_summary})
    asyncio.get_event_loop().run_until_complete(main())
