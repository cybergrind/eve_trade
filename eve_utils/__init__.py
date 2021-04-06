import asyncio
import logging

import httpx


TIMEOUT = 5
PARALLEL = 5
SEM = asyncio.Semaphore(PARALLEL)

log = logging.getLogger("eve_utils")



async def eve_get(url, params={}, timeout=TIMEOUT):
    async with SEM:
        while True:
            try:
                async with httpx.AsyncClient() as session:
                    resp = await session.get(url, params=params, timeout=timeout)
                limit = int(resp.headers.get('X-Esi-Error-Limit-Remain', 10))
                reset_time = int(resp.headers.get('X-Esi-Error-Limit-Reset', 10)) + 1
                log.debug(f'Limit {limit}')
            except (
                asyncio.TimeoutError,
                asyncio.CancelledError
            ):
                log.exception(f'Exception in: {url} {params}')
                await asyncio.sleep(10)
                continue
            if resp.status_code > 399:
                print(resp.headers)
                limit = int(resp.headers.get('X-Esi-Error-Limit-Remain', 10))
                reset_time = int(resp.headers.get('X-Esi-Error-Limit-Reset', 10)) + 1
                if limit > PARALLEL * 2:
                    log.debug(f'Limit {limit}')
                    await asyncio.sleep(reset_time / PARALLEL)
                else:
                    log.debug(
                        f'Bad status: {resp.status_code}. sleep for {reset_time} limit: {limit} '
                        f'url: {url}'
                    )
                    await asyncio.sleep(reset_time)
            if resp.status_code > 499:  # retry bad server
                log.debug(f'Retry: {url}/{params}')
                continue
            log.debug(f'Got resp: {url} {params}')
            return resp


def set_parallel(n):
    global PARALLEL, SEM
    PARALLEL = n
    SEM = asyncio.semaphore(PARALLEL)
