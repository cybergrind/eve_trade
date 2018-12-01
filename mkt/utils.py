from datetime import datetime

import numpy as np
import pandas as pd


def prepare_df(df, idx=None):
    df = df.copy()
    if idx is None:
        return df
    df = df.set_index(idx)
    # duplicates = order book was updated in the middle of process
    # df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True)
    return df


def diff_df(df1, df2, new_index=["order_id"]):
    a1 = prepare_df(df1, new_index)
    b1 = prepare_df(df2, new_index)

    ret = {"changed": [], "new": [], "deleted": []}

    c = pd.concat([a1, b1], keys=["old", "new"]).set_index("order_id")
    g = c.groupby(["order_id"])
    for order_id, rows in g:
        if len(rows) == 1:
            if rows.iloc[0].level_0 == "old":
                rows = rows.drop(["level_0"], axis=1)
                ret["deleted"].append(rows)
            else:
                rows = rows.drop(["level_0"], axis=1)
                ret["new"].append(rows)
        else:
            r = rows.drop(["level_0"], axis=1)
            if (r.iloc[0] == r.iloc[1]).all():
                continue
            else:
                ret["changed"].append(rows)
    return ret

    # delta = pd.concat([b1, a1, a1]).drop_duplicates(keep=False)

    return df1.values[delta], df2.values[delta]


def diff_df2(df1, df2):
    """
    diff_df2('books_history/20181201_1149_10000002.csv.gz', 'books_history/20181201_1154_10000002.csv.gz')
    """
    a1 = df1
    b1 = df2

    _new = np.setdiff1d(b1.index.values, a1.index.values)
    _new_set = b1.loc[_new]
    _del = np.setdiff1d(a1.index.values, b1.index.values)
    _del_set = a1.loc[_del]

    a2 = a1.drop(_del)
    b2 = b1.drop(_new)

    print(f'New: {len(_new)} Del: {len(_del)}')
    f = ['price', 'volume_remain']
    diff = (a2[f] != b2[f]).any(axis=1)
    return _new_set, _del_set, (b2[diff], a2[diff])


def book_from_csv(f_name):
    query = 'duration <= 90 & system_id in [30000142, 30000144]'
    df = pd.read_csv(f_name).query(query).set_index(['order_id'])
    df['issued'] = pd.to_datetime(df['issued'])
    return df


def set_type(a, b, fields, type, set_created=False):
    d = (a[fields] != b[fields]).all(axis=1)
    _all = a.loc[d[d].index.values]
    idx = d[d].index.values
    if len(idx) == 0:
        return a

    print(f'Set type: {type} => {len(_all)}')
    a.loc[idx, 'type'] = type
    if set_created:
        a.loc[idx, 'created'] = a.loc[idx, 'issued']
    return a


def build_diff(start, end):
    # a_name = 'books_history/20181201_1149_10000002.csv.gz'
    # b_name = 'books_history/20181201_1154_10000002.csv.gz'

    df1 = book_from_csv(start['name'])
    df2 = book_from_csv(end['name'])

    _new, _del, (_to, _from) = diff_df2(df1, df2)
    _new['date'] = end['date']
    _del['date'] = end['date']
    _to['date'] = end['date']
    _from['date'] = end['date']

    _new['created'] = _new['issued']
    _new['type'] = 'new'

    _del['type'] = 'deleted'

    # both / price / volume_remain
    _to['volume_change'] = _from['volume_remain'] - _to['volume_remain']
    _to['price_change'] = _from['price'] - _to['price']

    set_type(_to, _from, ['price'], 'price', set_created=True)
    set_type(_to, _from, ['volume_remain'], 'volume')
    set_type(_to, _from, ['price', 'volume_remain'], 'both', set_created=True)

    diff = pd.concat([_new, _del, _to])
    assert len(diff[diff.type.isna()]) == 0

    out = {'df1': df1, 'df2': df2, 'new': _new, 'del': _del, 'from': _from, 'to': _to, 'diff': diff}
    return out


def test_me():
    return build_diff(
        {'name': 'books_history/20181130_1841_10000002.csv.gz', 'date': datetime.now()},
        {'name': 'books_history/20181130_1842_10000002.csv.gz', 'date': datetime.now()},
    )
    return build_diff(
        {'name': 'books_history/20181201_1149_10000002.csv.gz', 'date': datetime.now()},
        {'name': 'books_history/20181201_1154_10000002.csv.gz', 'date': datetime.now()},
    )
