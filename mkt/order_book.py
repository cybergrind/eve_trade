import os
import json
import logging
import requests
import time
import pandas as pd


# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
log = logging.getLogger('order_book')

data = {
    'datasource': 'tranquility',
    'order_type': 'all',
}

EVERYSHORE = 10000037
FORGE = 10000002

JITA = 60003760
FORTIZAR_YE = 1021298932576
MOBILE = 33475

_cache = {}


def sort_book(book):
    out = {}

    for order in book:
        # if order['location_id'] not in [JITA, FORTIZAR_YE]:
        #     continue
        out.setdefault(order['type_id'], {}).setdefault(order['is_buy_order'], []).append(order)

    for v in out.values():
        if True in v:
            v[True] = sorted(v[True], key=lambda x: x['price'], reverse=True)
        if False in v:
            v[False] = sorted(v[False], key=lambda x: x['price'])
    return out


def get_book(region):
    book = []
    page = 1
    while True:
        url = f'https://esi.tech.ccp.is/latest/markets/{region}/orders/'
        resp = requests.get(url, params={**data, 'page': page})
        if resp.status_code not in (200, 304):
            print(url)
            print(resp.status_code)
            print(resp.json())
            break
        page += 1
        print(f'Book page: {page}')
        if not resp.json():
            log.debug('Pages: {}'.format(page))
            break
        book.extend(resp.json())
    return sort_book(book)


def init_books():
    for region in [EVERYSHORE, FORGE]:
        if region not in _cache:
            _cache[region] = get_book(region)


def save_book_cache():
    with open('book.tmp.json', 'w') as f:
        if not _cache:
            init_books()
        json.dump(_cache, f)
        os.rename('book.tmp.json', 'book.json')


def load_book_cache():
    global _cache
    try:
        with open('book.json') as f:
            _cache = json.load(f)
    except:
        pass


load_book_cache()


_type_ids = pd.read_pickle('types.pickle')


def get_name(type_id):
    if _type_ids.index.contains(type_id):
        return _type_ids.loc[type_id][0]
    return f'Unknown Name: {type_id}'


def compare_type(type_id=MOBILE, src=FORGE, dst=EVERYSHORE):
    from mkt.hist import get_history

    assert src != dst, (src, dst)
    # print(f'Process type: {type_id}')
    hsrc = get_history(type_id, src)[-8:]
    hdst = get_history(type_id, dst)[-8:]
    if type(hdst) == list or type(hsrc) == list:
        return {'error': 'got a list'}

    hsrc = hsrc.to_dict(orient='records')
    hdst = hdst.to_dict(orient='records')

    if len(hdst) < 8 or len(hsrc) < 8:
        return {'error': 'not liquid history'}
    # return if not liquid type
    count = 0
    vol = 0
    for src, dst in zip(hsrc, hdst):
        # print(f'{src} => {dst}')
        if src['date'] != dst['date']:
            return {'error': 'not volatile type'}

        diff = dst['average'] - src['average']
        day_vol = diff * dst['order_count']
        count += dst['order_count']
        # print(f'Day vol: {day_vol} Diff: {diff}')
        if day_vol < 10000:
            # print(f'Day vol: {type_id} {day_vol} / {count} {vol}')
            return {'error': f'not volatile type: day vol {day_vol} {dst}'}
        vol += day_vol
    assert vol > 20, vol
    return {'vol': vol * .94, 'count': count, 'type_id': type_id, 'name': get_name(type_id)}


PRICE_RANGE = {
    'min': 10000,
    'max': 50000000,
}


from multiprocessing import Pool

def get_best_rates(src=FORGE, dst=EVERYSHORE):
    src = str(src)
    dst = str(dst)
    out = []
    c = 0
    total = len(_cache[dst])

    params = []

    for type_id, book in _cache[dst].items():
        if type_id not in _cache[src]:
            continue
        # print(f'Process: {type_id} {c}/{total}')

        log.debug(f'Process: {type_id}')
        if len(book.keys()) != 2:
            continue

        sell, buy = book['false'], book['true']
        if not sell:
            continue
        order = sell[0]
        if PRICE_RANGE['max'] < order['price'] < PRICE_RANGE['min']:
            continue

        comp_type = compare_type(type_id, src, dst)
        if 'error' in comp_type:
            log.debug(f'Skip: {type_id} {comp_type}')
            continue
        # print(comp_type)
        assert comp_type['vol'] > 1
        out.append(comp_type)
    out = sorted(out, key=lambda x: x['vol'], reverse=True)
    return out


def main():
    save_book_cache()


if __name__ == '__main__':
    main()
