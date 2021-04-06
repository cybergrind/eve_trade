#!/usr/bin/env python3
import asyncio
import datetime
import logging
import os
from itertools import chain

import pandas as pd

from eve_utils import eve_get
from mkt.const import FORGE, TIME_PAT, STRUCTS

# expires: Sun, 25 Nov 2018 14:07:13 GMT
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("book_export")
SAVE_PAT = "%Y%m%d_%H%M"


REGIONS = [FORGE]
SAVE_PATH = "books_history"

os.makedirs(SAVE_PATH, exist_ok=True)
MAX_WAIT = 30
ORDER_PARAMS = {'datasource': 'tranquility', 'order_type': 'all'}


async def wait_pages(region):
    url = f"https://esi.evetech.net/latest/markets/{region}/orders/"
    while True:
        resp = await eve_get(url, ORDER_PARAMS)
        print(f'{resp.headers}')
        expires = datetime.datetime.strptime(resp.headers["expires"], TIME_PAT)
        delta = (expires - datetime.datetime.utcnow()).total_seconds()
        if 0 < delta < MAX_WAIT:
            log.debug(f"Sleep for: {delta}")
            await asyncio.sleep(abs(delta))
        else:
            return int(resp.headers["X-Pages"]), None


async def get_book_page(region, page):
    url = f"https://esi.evetech.net/latest/markets/{region}/orders/"
    resp = await eve_get(url, {"page": page, **ORDER_PARAMS})
    ret = resp.json()
    log.debug(f"Len ret: {len(ret)}")
    return ret


async def get_book_pages(region, pages):
    futs = []
    for page in range(1, pages + 1):
        futs.append(get_book_page(region, page))
    ok, errs = await asyncio.wait(futs, timeout=60)
    # log.debug(f"Finished wait {len(ok)}/{len(errs)} / {list(ok)[0]}")
    assert len(errs) == 0, errs
    return list(chain(*[task.result() for task in ok]))


def check_book(book):
    """
    if we have duplicates this means that we have orderbook from two different caches
    eg. from getting it in a middle of cache refresh
    """
    correct = not book.index.duplicated().any()
    log.debug(f'Book correctness: {correct}')
    return correct


async def get_book(region, pages):
    log.debug(f"Pages: {pages}")
    while True:
        book = await get_book_pages(region, pages)
        log.debug(f"Got book: {len(book)} {book[0]}")
        df = pd.DataFrame.from_dict(book).set_index(["order_id"])
        df.sort_index(inplace=True)
        dt = datetime.datetime.utcnow().strftime(SAVE_PAT)
        if check_book(df):
            break
    path = f"{SAVE_PATH}/{dt}_{region}.csv.gz"
    log.debug(f'Save book: {path}')
    df.to_csv(path, compression="gzip")


async def book_worker():
    while True:
        for region in REGIONS:
            try:
                pages, expires = await wait_pages(region)
                await get_book(FORGE, pages)
                log.debug(f"Expires: {expires}")
            except Exception as e:
                log.exception(f'When downloaded the book: {e}')
                log.info(f'Sleep for 1 minute')
                await asyncio.sleep(60)

        if not expires:
            continue

        delta = (expires - datetime.datetime.utcnow()).total_seconds()
        while delta > 0:
            log.debug(f"delta: {delta}")
            await asyncio.sleep(5)
            delta = (expires - datetime.datetime.utcnow()).total_seconds()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(book_worker())


if __name__ == "__main__":
    main()
