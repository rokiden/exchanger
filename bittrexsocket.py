import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from base64 import b64decode
from typing import Optional, Callable
from zlib import decompress, MAX_WBITS

from signalr_aio import Connection

import corotools
from watchdog import Watchdog

log = logging.getLogger(__name__)

URL = 'https://socket-v3.bittrex.com/signalr'
HEARTBEAT_TIMEOUT = 6


class BittrexSocket:
    __hub = None
    __invoke_event = None
    __invoke_resp = None
    __watchdog: Optional[Watchdog]
    __invoke_lock = Optional[asyncio.Lock]
    __connection: Optional[Connection]

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret

    async def listen(self, channels: list[str], callbacks: dict[str, Callable]):
        self.__watchdog = Watchdog(HEARTBEAT_TIMEOUT)
        self.__invoke_lock = asyncio.Lock()
        await self.__connect()
        if self.api_key is not None:
            await self.__auth()

        assert 'heartbeat' not in channels
        channels.append('heartbeat')
        callbacks['heartbeat'] = self.__on_heartbeat
        await self.__subscribe(channels, callbacks)
        await self.__watchdog.loop()
        self.__connection.close()

    async def __on_heartbeat(self, _):
        self.__watchdog.reset()

    async def __connect(self):
        self.__connection = Connection(URL)
        self.__hub = self.__connection.register_hub('c3')
        self.__connection.received += self.__on_message
        self.__connection.error += self.__on_error
        self.__connection.start()
        log.info(f'Connected')

    async def __on_message(self, **msg):
        if 'R' in msg:
            self.__invoke_resp = msg['R']
            self.__invoke_event.set()

    def stop(self):
        self.__watchdog.stop()

    async def __on_error(self, msg):
        log.error(str(msg))
        self.stop()

    async def __auth(self):
        timestamp = str(int(time.time()) * 1000)
        random_content = str(uuid.uuid4())
        content = timestamp + random_content
        signed_content = hmac.new(self.api_secret.encode(), content.encode(), hashlib.sha512).hexdigest()
        response = await self.__invoke('Authenticate', self.api_key, timestamp, random_content, signed_content)

        if response['Success']:
            log.info(f'Authenticated')

            async def reauth(_):
                asyncio.create_task(self.__auth())

            self.__hub.client.on('authenticationExpiring', reauth)
        else:
            log.error(f'Authentication failed: {response["ErrorCode"]}')

    async def __subscribe(self, channels, callbacks):
        for method, callback in callbacks.items():
            self.__hub.client.on(method, corotools.wraptry(corotools.wrapfunc(callback, self.__decode_message),
                                                           msg='BittrexSocket callback exception'))

        response = await self.__invoke('Subscribe', channels)
        for i in range(len(channels)):
            if response[i]['Success']:
                log.info(f'Subscription to "{channels[i]}" successful')
            else:
                log.error(f'Subscription to "{channels[i]}" failed: {response[i]["ErrorCode"]}')
                self.stop()

    async def __invoke(self, method, *args):
        async with self.__invoke_lock:
            self.__invoke_event = asyncio.Event()
            self.__hub.server.invoke(method, *args)
            await self.__invoke_event.wait()
            return self.__invoke_resp

    @staticmethod
    def __decode_message(msg):
        if not len(msg):
            return None
        else:
            msg = msg[0]
        try:
            decompressed_msg = decompress(b64decode(msg, validate=True), -MAX_WBITS)
        except SyntaxError:
            decompressed_msg = decompress(b64decode(msg, validate=True))
        return json.loads(decompressed_msg.decode())
