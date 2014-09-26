from __future__ import unicode_literals

import calendar
import collections
import contextlib
import datetime
import io
import json
import os.path
import re
import sys
import time

import progress.bar

from .compat import compat_str
from .dbhelpers import DBConnection


class keydefaultdict(collections.defaultdict):

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


class NoProgress(object):

    def __init__(self, stream):
        pass

    def update(self):
        pass

    def finish(self):
        pass


class FileProgress(object):

    def __init__(self, stream):
        pos = stream.tell()
        stream.seek(0, 2)
        self.size = stream.tell()
        stream.seek(pos, 0)
        self.bar = progress.bar.Bar(
            '', max=self.size, suffix='%(percent)d%% ETA %(eta)ds')
        self.stream = stream

        self.update_every = 1000
        self._update_counter = 0

    def update(self):
        self._update_counter += 1
        if self._update_counter % self.update_every != 0:
            return
        pos = self.stream.tell()
        self.bar.goto(pos)

    def finish(self):
        self.bar.finish()


class ProgressBar(progress.bar.Bar):
    def __init__(self, *args, update_every=10000, **kwargs):
        super(ProgressBar, self).__init__(*args, **kwargs)
        self.update_every = update_every
        self._skipped_updates = 0

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.finish()
        return

    def next(self, count=1):
        self._skipped_updates += count
        if (self._skipped_updates < self.update_every and
                self.remaining > self.update_every):
            return
        up = self._skipped_updates
        self._skipped_updates = 0
        return super(ProgressBar, self).next(up)


class Option(object):

    def __init__(self, name, **kwargs):
        self.name = name
        assert 'dest' in kwargs
        self.kwargs = kwargs


def options(option_list=[], requires_db=True):
    def wrapper(func):
        def outfunc(args):
            if requires_db:
                config = read_config(args)
                with DBConnection(config) as db, DBConnection(config) as wdb:
                    func(args, config, db, wdb)
            else:
                return func(args)
        outfunc.option_list = option_list
        outfunc.__name__ = func.__name__
        return outfunc
    return wrapper


def read_config(args):
    with io.open(args.config_filename, 'r', encoding='utf-8') as configf:
        return json.load(configf)


def write_excel(filename, data, headers=None):
    import xlsxwriter
    with contextlib.closing(xlsxwriter.Workbook(filename)) as workbook:
        worksheet = workbook.add_worksheet()
        bold = workbook.add_format({'bold': 1})

        rowidx = 0
        maxwidths = [len(compat_str(d)) for d in data[0]]
        if headers is not None:
            for col, h in enumerate(headers):
                maxwidths[col] = max(maxwidths[col], len(compat_str(h)))
                worksheet.write(rowidx, col, h, bold)
            rowidx += 1

        for rowidx, row in enumerate(data, start=rowidx):
            for colidx, d in enumerate(row):
                maxwidths[colidx] = max(maxwidths[colidx], len(compat_str(d)))
                worksheet.write(rowidx, colidx, d)

        for colidx, mw in enumerate(maxwidths):
            worksheet.set_column(colidx, colidx, mw)


def gen_random_numbers(rnd, minv, maxv, count):
    assert maxv - minv >= count
    res = set()
    while len(res) < count:
        res.add(rnd.randint(minv, maxv))
    return list(res)


def parse_date(s):
    d = datetime.datetime.strptime(s, '%Y-%m-%d')
    return calendar.timegm(d.utctimetuple())


def timestamp_str(ts):
    st = time.gmtime(ts)
    return time.strftime('%Y-%m-%d', st)


def datetime_str(dt):
    return dt.strftime('%Y-%m-%d')


def get_table_size(db, table):
    fn = os.path.join('.cache', 'size-' + table)
    try:
        with io.open(fn, encoding='ascii') as inf:
            res = int(inf.read())
            if res > 10:  # Maybe still in development mode?
                return res
    except IOError:
        pass

    try:
        sys.stdout.write('Calculating ETA ...')
        sys.stdout.flush()
        count = db.simple_query('SELECT COUNT(*) FROM ' + table)[0]
        assert isinstance(count, int)
    finally:
        sys.stdout.write('\r\x1b[K')
        sys.stdout.flush()

    if not os.path.exists('.cache'):
        os.mkdir('.cache')
    with io.open(fn, 'w', encoding='ascii') as outf:
        outf.write(compat_str(count))
    return count


class TableSizeProgressBar(ProgressBar):
    def __init__(self, db, table, description, **kwargs):
        count = get_table_size(db, table)
        super(TableSizeProgressBar, self).__init__(
            description, max=count,
            suffix='%(index)d/%(max)d %(percent)d%% ETA %(eta)ds',
            **kwargs)
        db.register_bar(self)


_user_rex = re.compile(r'[a-f0-9]{40}([^!]+)!userid_type:unicode')


def extract_user_from_cookies(cookies, default=None):
    m = _user_rex.search(cookies)
    if m:
        return m.group(1)
    return default


def sql_filter(name, config):
    filter_sql = config.get('%s_extra_filter' % name)
    if filter_sql:
        return filter_sql
    return ''
