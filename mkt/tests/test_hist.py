
import pandas as pd
from mkt.hist import columns


def test_01_caching():
    data = [
        {
            "region": 1,
            "type_id": 2,
            "data": [
                {
                    "average": 2000000.0,
                    "date": "2017-10-03",
                    "highest": 2000000.0,
                    "lowest": 2000000.0,
                    "order_count": 2,
                    "volume": 3,
                },
                {
                    "average": 2000000.0,
                    "date": "2017-10-04",
                    "highest": 2000000.0,
                    "lowest": 2000000.0,
                    "order_count": 1,
                    "volume": 1,
                },
            ],
        }
    ]
    frame = pd.DataFrame(data, columns=columns)
    import ipdb; ipdb.set_trace()
