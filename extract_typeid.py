
# published=True
# marketGroupID

# - groups
# iconID: 79
# groupID: 525
# groupID: 349

# marketGroupID: 792
# marketGroupID: 793


#!/usr/bin/env python3
import argparse
import logging
import pickle

import yaml

import pandas

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("extract_typeid")


def parse_args():
    parser = argparse.ArgumentParser(description="DESCRIPTION")
    # parser.add_argument('-l', '--ll', dest='ll', action='store_true', help='help')
    parser.add_argument("typefile", default="sde/fsd/typeIDs.yaml")
    return parser.parse_args()


def should_skip(dct):
    if "name" not in dct or "en" not in dct["name"]:
        return True


def extract_typeid(args):
    out = []
    with open(args.typefile) as f:
        for k, v in yaml.load(f).items():
            if should_skip(v):
                continue
            out.append({"id": str(k), "name": v["name"]["en"]})
    return out


def main():
    args = parse_args()
    extracted = extract_typeid(args)
    df = pandas.DataFrame(extracted).set_index(["id"])
    with open("types.pickle", "wb") as f:
        pickle.dump(df, f)


if __name__ == "__main__":
    main()
