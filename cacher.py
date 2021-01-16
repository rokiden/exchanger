import asyncio
import logging
import time
from collections import deque

DROP_KEYWORD = 'cache_drop'
LIFETIME_KEYWORD = 'cache_lifetime'

log = logging.getLogger(__name__)


class Cacher:
    def __init__(self, deflifetime, maxlen=None):
        self.deflifetime = deflifetime
        self.cache: dict[int, (float, object)] = {}
        self.cache_queue = deque(maxlen=maxlen) if maxlen is not None else None

    def __call__(self, fn):
        def pre_cache(args, kwargs):
            if DROP_KEYWORD in kwargs:
                drop = kwargs[DROP_KEYWORD]
                del kwargs[DROP_KEYWORD]
            else:
                drop = False

            if LIFETIME_KEYWORD in kwargs:
                lifetime = kwargs[LIFETIME_KEYWORD]
                del kwargs[LIFETIME_KEYWORD]
            else:
                lifetime = self.deflifetime

            cache_key = self._args_to_hash(args, kwargs)
            cached_time_val = self.cache[cache_key] if cache_key in self.cache else None
            cached_valid = (not drop and time.time() < cached_time_val[0] + lifetime) if cached_time_val else None

            if self.cache_queue is not None:
                if cache_key in self.cache_queue:
                    self.cache_queue.remove(cache_key)
                elif len(self.cache_queue) == self.cache_queue.maxlen:
                    del self.cache[self.cache_queue.popleft()]
                self.cache_queue.append(cache_key)

            return cache_key, cached_valid, (cached_time_val[1] if cached_time_val else None)

        def post_cache(key, value):
            self.cache[key] = (time.time(), value)

        def wrapped(*args, **kwargs):
            key, valid, value = pre_cache(args, kwargs)
            log.debug(f'Cache {valid} {fn}')
            if valid:
                result = value
            else:
                result = fn(*args, **kwargs)
                post_cache(key, result)
            return result

        async def wrapped_async(*args, **kwargs):
            key, valid, value = pre_cache(args, kwargs)
            log.debug(f'Cache {valid} {fn}')
            if valid:
                result = value
            else:
                result = await fn(*args, **kwargs)
                post_cache(key, result)
            return result

        return wrapped_async if asyncio.iscoroutinefunction(fn) else wrapped

    @staticmethod
    def _args_to_hash(args, kwargs):
        return hash(str(args) + str(kwargs))