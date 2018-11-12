import asyncio
import logging
import os
import traceback
import datetime

import aiohttp
import pandas as pd

from mkt.order_book import EVERYSHORE, FORGE


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
log = logging.getLogger('hist')


DATE_PAT = datetime.datetime.now().strftime('%Y%m%d')
FNAME = f'hist_cache_{DATE_PAT}.pickle'
TMP = 'hist_cache.tmp.pickle'
columns = ['region', 'type_id', 'average', 'date', 'highest', 'lowest',
           'order_count', 'volume']

try:
    _hist_cache = pd.read_pickle(FNAME)
except:
    log.exception('ooo')
    # init with some data to make contains work
    data = [{'region': 'default',
             'type_id': 'default',
             'date': 'default'}]
    _hist_cache = pd.DataFrame(data=data, columns=columns).set_index(['region', 'type_id'])


def save_hist_cache():
    _hist_cache.to_pickle(TMP)
    os.rename(TMP, FNAME)


_sc = 0


def maybe_save():
    global _sc
    _sc += 1
    if _sc > 100:
        save_hist_cache()
        _sc = 0


semaphore = asyncio.Semaphore(25)


def limit_check(resp):
    limit = int(resp.headers['X-Esi-Error-Limit-Remain'])

    if limit < 85:
        print(f'Limit reached: {resp}')
        loop = asyncio.get_event_loop()
        loop.stop()


def get_history(type_id, region):
    type_id = str(type_id)
    region = str(region)
    if _hist_cache.index.contains((region, type_id)):
        return _hist_cache.loc[region, type_id]
    return []


async def _get_history(session, type_id, region):
    global _hist_cache

    type_id = str(type_id)
    region = str(region)
    if _hist_cache.index.contains((region, type_id)):
        print(f'Cached: {region} => {type_id}')
        return None

    async with semaphore:
        try:
            data = {
                'region': region,
                'type_id': type_id,
                'datasource': 'tranquility',
            }
            url = f'https://esi.tech.ccp.is/latest/markets/{region}/history/'
            ret = {}
            while isinstance(ret, dict):
                while True:
                    resp = await session.get(url, params=data)
                    limit_check(resp)
                    if resp.status == 502:
                        sleep = int(resp.headers['X-Esi-Error-Limit-Reset'])
                        print(f'Bad gateway. sleep for {sleep}')
                        await asyncio.sleep(sleep)
                        continue
                    break
                # print(resp)
                assert resp.status in (200, 404), (resp, resp.reason)
                if resp.status == 404:
                    return None

                ret = await resp.json()
                if 'error' in ret:
                    print(f'Error history: {ret} Type: {type_id} in {region}')
                    await asyncio.sleep(1)
                print(f'Processed: {type_id} in {region}')
            data = [{'region': region, 'type_id': type_id, 'data': ret}]
            return data
            await asyncio.sleep(1.1)
            return ret
        except Exception as e:
            traceback.print_exc()
            # print(data)
            loop = asyncio.get_event_loop()
            loop.stop()


async def region_cache(region, types):
    global _hist_cache

    with aiohttp.ClientSession() as session:
        futs = [_get_history(session, type_id, region) for type_id in types]
        ok, errs = await asyncio.wait(futs)
        data = []
        for task in ok:
            res = task.result()
            if res:
                data.extend(res)
        frame = pd.DataFrame(data, columns=columns)

        together = pd.concat([_hist_cache.reset_index(), frame])
        _hist_cache = together.set_index(['region', 'type_id'])

        save_hist_cache()


def batches(items, size=1000):
    batch = []
    for i in items:
        batch.append(i)
        if len(batch) == size:
            yield batch
            batch = []
    yield batch


def update_cache(books):
    loop = asyncio.get_event_loop()
    for region, book in books.items():
        print('Run for: {}'.format(region))
        types = list(book.keys())
        for batch in batches(types):
            loop.run_until_complete(region_cache(region, batch))


def main():
    from mkt.order_book import _cache
    update_cache(_cache)


if __name__ == '__main__':
    main()
