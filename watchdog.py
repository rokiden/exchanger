import asyncio
import time


class Watchdog:
    __last = None
    __stop_event = None

    def __init__(self, timeout):
        self.__timeout = timeout
        self.__stop_event = asyncio.Event()

    async def loop(self):
        if self.__last is None:
            self.__last = time.time()

        while True:
            try:
                await asyncio.wait_for(self.__stop_event.wait(), 1)
                print('Watchdog stopped')
                break
            except asyncio.TimeoutError:
                if time.time() > self.__last + self.__timeout:
                    print('Watchdog timeout')
                    break

    def reset(self):
        self.__last = time.time()

    def stop(self):
        self.__stop_event.set()
