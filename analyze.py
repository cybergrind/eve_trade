import datetime
from collections import namedtuple

import pandas as pd


START = datetime.datetime(2021, 4, 6, 12, 19)
names = pd.read_hdf('types.hdf')
df = pd.read_hdf('diff.hdf')
df = df.reset_index()
df['type_name'] = names.reindex(df.type_id).reset_index().name
dfo = df.copy()
df = df.drop(['issued', 'location_id', 'duration', 'system_id'], axis=1)

changed = df[(df.volume_change > 1) | (df.type == 'deleted')]

by_type = changed.groupby('type_id')['type_id'].count()
more_than_3 = by_type[by_type > 1]

u_cols = [
    'is_buy_order',
    'type_id',
    'volume_change',
    'type_name',
]


# .drop_duplicates(['type_id', 'is_buy_order'])
df = df.set_index(['type_id'])


def get_price_old(row):
    type_id = row.name
    is_buy = row.is_buy_order
    t = df.reset_index().set_index(['type_id', 'is_buy_order'])
    b = t.loc[type_id, is_buy]
    idx = 0 if is_buy else -1
    try:
        return b[b.type == 'volume'].sort_values('date').iloc[idx].price
    except Exception:
        return 0.0001


def get_price(frame):
    b = df.reset_index().set_index(['type_id', 'is_buy_order'])
    b = b[b.type == 'volume'].sort_values('date')
    aprice = (
        b.groupby(['type_id', 'is_buy_order'])
        .tail(50)
        .groupby(['type_id', 'is_buy_order'])
        .mean()
        .price.round(1)
    )

    b = b.reset_index().drop_duplicates(['type_id', 'is_buy_order'], keep='last')
    b = b.set_index(['type_id', 'is_buy_order'])
    rei = b.reindex([frame.type_id, frame.is_buy_order]).reset_index()
    frame['price'] = rei.price
    frame = frame.set_index(['type_id', 'is_buy_order'])
    frame['aprice'] = aprice.reindex(frame.index)
    frame = frame.reset_index()
    return frame


def get_movers():
    tmp = changed[changed.type_id.isin(more_than_3.index)].reset_index()[u_cols].copy()
    tmp = tmp.groupby(['type_id', 'is_buy_order', 'type_name']).sum().reset_index()
    tmp = get_price(tmp)
    tmp = tmp.set_index('type_id')
    return tmp


big_movers = get_movers()
with_price_changed = big_movers.reset_index()
# with_price_changed = big_movers[(big_movers.volume_change != 0) & (big_movers.price > 100000)]

### with_price_changed = with_price_changed.reset_index()
### with_price_changed['type_name'] = names.loc[with_price_changed['type_id']].reset_index().name
### with_price_changed.set_index(['order_id'])

# expensive = (
#   with_price_changed[['type_id', 'type_name', 'price', 'is_buy_order']]
#   .sort_values(['price'])
#   .iloc[-30:]
# )


def moves(type_id):
    return df[(df.type_id == type_id) & df.volume_change]


def analyze():
    tmp = with_price_changed.set_index(['type_id'])
    # locate_res = tmp.apply(lambda x: locate(x.name), axis=1)
    # tmp['ratio'] = locate_res.map(lambda x: x.ratio)
    # tmp['vol_ratio'] = locate_res.map(lambda x: x.vol_ratio)
    # tmp['sell_vol'] = locate_res.map(lambda x: x.sell_vol)
    b = tmp.loc[tmp.is_buy_order]
    s = tmp.loc[~tmp.is_buy_order]
    tmp['ratio'] = ((s.price - b.price) / b.price).round(2)
    tmp['vol_ratio'] = (s.volume_change / b.volume_change).round(2)
    tmp['sell_vol'] = s.volume_change

    tmp = tmp.dropna(0)  # remove nans in ratio
    return tmp.sort_values(['ratio'])


a = analyze()
b = a[(a.price > 100000) & (a.ratio > 1.15)]
tb = b[b.is_buy_order & (b.price < b.aprice)]
ts = b[~b.is_buy_order & (b.price > b.aprice)]


def pp(frame):
    return frame[
        [
            'is_buy_order',
            'created',
            'date',
            'price',
            'volume_change',
            'type_name',
        ]
    ]


def ppf(type_id, is_buy=True):
    out = pp(df.loc[(df.type_id == type_id) & (df.is_buy_order == is_buy)])
    print(f'SumVol: {out["volume_change"].sum()}')
    return out


def ppb(type_id):
    return ppf(type_id, is_buy=True)


def pps(type_id):
    return ppf(type_id, is_buy=False)


def pp_all(df):
    for i in range(100000):
        o = df.iloc[i * 50 : (i + 1) * 50]
        if o.size == 0:
            return
        print(o)


Pair = namedtuple('Pair', ['buy', 'sell'])


def bs(type_id):
    z = df.loc[type_id]
    return Pair(z[z.is_buy_order], z[~z.is_buy_order])


def sales(type_id):
    z = df.loc[type_id]
    return z[z.is_buy_order & (z.type.isin(('volume', 'deleted')))]


def ex(type_id):
    t = df.loc[type_id]
    t = t.loc[t.date > START]
    print(f'Type: {t.iloc[0].type_name} => {t.iloc[0].name}')
    return t[
        [
            'order_id',
            'is_buy_order',
            'price',
            'volume_remain',
            'volume_total',
            'date',
            'type',
            'volume_change',
            'price_change',
        ]
    ]


def by_50(frame, back=True):
    c = 0

    while True:
        if back:
            i1 = -50 * (c + 1)
            i2 = -50 * c or None
        else:
            i1 = 50 * c or None
            i2 = 50 * (c + 1)
        yield frame.iloc[i1:i2]
        c += 1


# by_order = changed.groupby('order_id')['order_id'].count()
