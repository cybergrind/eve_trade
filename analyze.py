import pandas as pd

names = pd.read_hdf('types.hdf')
df = pd.read_hdf('diff.hdf')
df = df.reset_index()
df['type_name'] = names.reindex(df.type_id).reset_index().name
df.set_index(['order_id'])


changed = df[df.volume_change > 1]

by_type = changed.groupby('type_id')['type_id'].count()
more_than_3 = by_type[by_type > 3]

big_movers = changed[changed.type_id.isin(more_than_3.index)]
with_price_changed = big_movers[(big_movers.price_change != 0) & (big_movers.price > 100000)]

# with_price_changed = with_price_changed.reset_index()
# with_price_changed['type_name'] = names.loc[with_price_changed['type_id']].reset_index().name
# with_price_changed.set_index(['order_id'])

expensive = (
    with_price_changed[['type_id', 'type_name', 'price', 'is_buy_order']]
    .sort_values(['price'])
    .iloc[-30:]
)


def locate(type_id):
    out = df[df.type_id == type_id]
    buy = out[out.is_buy_order].sort_values(['price']).iloc[-3:]
    sell = out[~out.is_buy_order].sort_values(['price']).iloc[:3]
    try:
        bp = buy.iloc[-1].price
        ratio = (sell.iloc[0].price - bp) / bp
    except Exception:
        ratio = 0.001
    return buy, sell, ratio


def moves(type_id):
    return df[(df.type_id == type_id) & df.volume_change]


def analyze():
    tmp = with_price_changed.set_index(['type_id'])
    tmp['ratio'] = tmp.apply(lambda x: locate(x.name)[2], axis=1)
    return tmp.sort_values(['ratio'])


# by_order = changed.groupby('order_id')['order_id'].count()
