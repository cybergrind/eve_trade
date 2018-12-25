import asyncio
import logging

import aiohttp

TIMEOUT = 5
PARALLEL = 5
SEM = asyncio.Semaphore(PARALLEL)
session = aiohttp.ClientSession()
session.ready = False

log = logging.getLogger("eve_utils")


async def session_reinit():
    global session
    old_session = session

    new_session = aiohttp.ClientSession()
    await new_session.__aenter__()
    new_session.ready = True

    session = new_session
    try:
        await old_session.__aexit__(None, None, None)
    except Exception as e:
        log.exception(e)


async def eve_get(url, params={}, timeout=TIMEOUT):
    if not session.ready:
        await session.__aenter__()
        session.ready = True

    async with SEM:
        while True:
            try:
                resp = await session.get(url, params=params, timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                log.exception(f'Exception in: {url} {params}')
                await session_reinit()
                continue
            if resp.status > 399:
                print(resp.headers)
                limit = int(resp.headers.get('X-Esi-Error-Limit-Remain', 10))
                reset_time = int(resp.headers.get('X-Esi-Error-Limit-Reset', 10)) + 1
                if limit > PARALLEL * 2:
                    log.debug(f'Limit {limit}')
                    await asyncio.sleep(reset_time / PARALLEL)
                else:
                    log.debug(f'Bad status: {resp.status}. sleep for {reset_time} limit: {limit}')
                    await asyncio.sleep(reset_time)
            if resp.status > 499:  # retry bad server
                log.debug(f'Retry: {url}/{params}')
                continue
            log.debug(f'Got resp: {url} {params}')
            return resp


def set_parallel(n):
    global PARALLEL, SEM
    PARALLEL = n
    SEM = asyncio.semaphore(PARALLEL)
