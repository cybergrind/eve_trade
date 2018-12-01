#!/usr/bin/env python3
import argparse
import logging
import os
import time
from datetime import datetime
from glob import glob

import pandas as pd

from mkt.utils import build_diff

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
log = logging.getLogger('build_diffs')


def parse_args():
    parser = argparse.ArgumentParser(description='DESCRIPTION')
    # parser.add_argument('-l', '--ll', dest='ll', action='store_true', help='help')
    return parser.parse_args()


class Snapshot:
    def __init__(self, df):
        self.df = df


class DiffManager:
    """
    can use: 20181129_1004_10000002.csv.gz
    """

    DIFFS_DIR = '.diffs'
    BOOKS_DIR = 'books_history'
    BOOK_PAT = f'{BOOKS_DIR}/%Y%m%d_%H%M_10000002.csv.gz'
    DIFF_PAT = '{from}_to_{to}.pickle'
    DATE_PAT = "%Y%m%d_%H%M"

    diff_path = 'diff.hdf'

    os.makedirs(DIFFS_DIR, exist_ok=True)
    # min_date = datetime(2018, 11, 29, 10, 00)

    def __init__(self, args):
        self.min_date = datetime.min
        self.init_books()
        self.init_diff()

    def init_books(self):
        fnames = []
        for fname in glob(f'{self.BOOKS_DIR}/*.csv.gz'):
            dt = datetime.strptime(fname, self.BOOK_PAT)
            if dt >= self.min_date:
                fnames.append({'name': fname, 'date': dt})
        fnames.sort(key=lambda x: x['date'], reverse=False)
        self.books = fnames

    def init_diff(self):
        if os.path.exists(self.diff_path):
            self.diff = pd.read_hdf(self.diff_path)
            self.min_date = self.diff.date.max()
            self.books = [x for x in self.books if x['date'] >= self.min_date]
            return

        first_book = self.books[0]
        self.diff = pd.read_csv(first_book['name']).set_index(['order_id'])
        self.diff['date'] = first_book['date']
        self.diff['type'] = 'init'
        self.diff['issued'] = pd.to_datetime(self.diff['issued'])

    def add_names(self):
        self.names = pd.read_hdf('types.hdf')
        self.diff['type_name'] = self.names.loc[self.diff['type_id']].reset_index().name

    def build_new_diffs(self):
        for s, e in zip(self.books, self.books[1:]):
            self.build_diff(s, e)

    def build_diff(self, start, end):
        t = time.time()
        log.debug(f'Build diff: {start} => {end}')
        diff = build_diff(start, end)['diff']
        log.debug(f'Diff len: {len(diff)}')
        self.diff = pd.concat([self.diff, diff])
        self.diff.to_hdf('diff.hdf', 'diff', mode='w')
        # both / price / volume_remain
        log.debug(f'Total time: {time.time() - t}')


def main():
    args = parse_args()

    DiffManager(args).build_new_diffs()


if __name__ == '__main__':
    main()
