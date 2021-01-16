import asyncio
import logging


async def periodic(func, period, *args, **kwargs):
    while True:
        task = asyncio.create_task(func(*args, **kwargs))
        await asyncio.sleep(period)
        if not task.done():
            raise RuntimeError('Periodic error')


def wrapmulti(corofuncs):
    async def wrapmulti_wrpd(*args, **kwargs):
        await asyncio.wait([c(*args, **kwargs) for c in corofuncs])

    return wrapmulti_wrpd


wraptrylog = logging.getLogger('wraptry')


def wraptry(corofunc, msg='wraptry'):
    async def wraptry_wrpd(*args, **kwargs):
        try:
            return await corofunc(*args, **kwargs)
        except Exception:
            wraptrylog.exception(msg)

    return wraptry_wrpd


def wrapfunc(corofunc, wrapper):
    async def wrapfunc_wrpd(*args, **kwargs):
        return await corofunc(wrapper(*args, **kwargs))

    return wrapfunc_wrpd
