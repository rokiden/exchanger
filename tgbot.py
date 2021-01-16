import collections.abc
import logging

from aiogram import Bot, Dispatcher, types, filters

log = logging.getLogger(__name__)


class TGBot:
    def __init__(self, token, user_id, callbacks: dict[str, collections.abc.Callable] = None):
        self.__token = token
        self.user_id = user_id
        self.api = Bot(token=self.__token)
        self.__dp = Dispatcher(self.api)
        self.__callbacks = callbacks if callbacks is not None else {}
        self.__callbacks['ping'] = self.__on_ping
        self.__dp.register_message_handler(self.__on_command, filters.IDFilter(self.user_id),
                                           commands=self.__callbacks.keys())

    async def polling(self):
        user = await self.api.me
        log.info(f"Bot: {user.full_name} [@{user.username}]")
        await self.__dp.skip_updates()
        log.info(f'Updates were skipped successfully.')
        await self.__dp.start_polling()

    async def stop(self):
        self.__dp.stop_polling()
        await self.__dp.wait_closed()

    async def __on_command(self, message: types.Message):
        cmd = message.get_command()
        log.debug('Command ' + message.text)
        cmd = cmd[1:]
        if cmd in self.__callbacks:
            try:
                await self.__callbacks[cmd](message)
            except Exception:
                log.warning('Callback exception', exc_info=True)
        else:
            log.error('Unknown cmd ' + cmd)

    async def __on_ping(self, message: types.Message):
        await message.answer('pong')
