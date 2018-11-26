import pandas as pd


def prepare_df(df, idx=None):
    df = df.copy()
    if idx is None:
        return df
    df = df.reset_index().set_index(idx)
    df.sort_index(inplace=True)
    return df


def diff_df(df1, df2, new_index=["order_id"]):
    a1 = prepare_df(df1, new_index)
    b1 = prepare_df(df2, new_index)

    ret = {"changed": [], "new": [], "deleted": []}

    c = pd.concat([a1, b1], keys=["old", "new"]).reset_index().set_index("order_id")
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
