# encoding: utf-8

from __future__ import unicode_literals

import argparse
import collections
import csv
import io
import json
import random
import re
import sys
import time

from . import sources
from .util import (
    FileProgress,
    extract_user_from_cookies,
    gen_random_numbers,
    get_table_size,
    Option,
    options,
    parse_date,
    ProgressBar,
    read_config,
    TableSizeProgressBar,
    timestamp_str,
    datetime_str,
    write_excel,
)
from .dbhelpers import (
    DBConnection,
)
from .hhu_actions import action_dennis_daily_stats


def read_requestlog_all(args, **kwargs):
    if args.files:
        for fn in args.files:
            with open(fn, 'rb') as inf:
                for r in sources.read_requestlog(inf, **kwargs):
                    yield r
    else:
        for r in sources.read_requestlog(sys.stdin.buffer, **kwargs):
            yield r


@options([
    Option('--format', dest='format', metavar='FORMAT',
           help='Output format ("repr", "json", or "benchmark")',
           default='repr')
])
def action_file_listrequests(args):
    """ Output all HTTP requests """

    format = args.format
    if format == 'repr':
        for req in read_requestlog_all(args):
            print(req)
    elif format == 'json':
        l = [req._asdict() for req in read_requestlog_all(args)]
        info = {
            '_format': 'requestlist',
            'requests': l,
        }
        json.dump(info, sys.stdout)
        print()  # Output a final newline
    elif format == 'benchmark':
        start_time = time.clock()
        count = sum(1 for _ in read_requestlog_all(args))
        end_time = time.clock()
        print(
            'Read %d requests in %d seconds (%d requests/s)' %
            (count, (end_time - start_time), count / (end_time - start_time)))
    elif format is None:
        raise ValueError('No format specified')
    else:
        raise ValueError('Invalid list format %r' % format)


@options([])
def action_load_requestlog(args):
    """ Load requestlog into the database """

    if not args.discardfile:
        raise ValueError('Must specify a discard file!')

    config = read_config(args)

    with io.open(args.discardfile, 'w', encoding='utf-8') as discardf, \
            DBConnection(config) as db:

        def discard(line):
            discardf.write(line)

        db.execute('''DROP TABLE IF EXISTS requestlog2;''')
        db.execute('''CREATE TABLE requestlog2 (
            id int PRIMARY KEY auto_increment,
            access_time int,
            ip_address varchar(255),
            request_url text,
            cookies text,
            user_agent text,
            deleted boolean NOT NULL,
            method varchar(10));
        ''')

        for r in read_requestlog_all(args, discard=discard,
                                     progressclass=FileProgress):
            sql = '''INSERT INTO requestlog2
                SET access_time = %s,
                    ip_address = %s,
                    request_url = %s,
                    cookies = %s,
                    user_agent = %s,
                    method = %s;
            '''
            db.execute(
                sql,
                (r.time, r.ip, r.path, r.cookies, r.user_agent, r.method))
        db.commit()


@options([
    Option('--xlsx-file', dest='xlsx_file', metavar='FILENAME',
           help='Name of the Excel file to write')
])
def action_dischner_nametable(args):
    """ Create a list of names and user IDs and write is as xlsx """

    config = read_config(args)
    if not args.xlsx_file:
        raise ValueError('Must specify an output file!')

    from .hhu_specific import get_status_groups

    with DBConnection(config) as db:
        status_groups = get_status_groups(db)
        db.execute('SELECT id, display_name FROM user where id != 1')
        rows = list(db)

        rnd = random.Random(123)
        numbers = gen_random_numbers(rnd, 0, 999999, len(rows))

        headers = ('ID', 'Name', 'Statusgruppe')
        tbl = [(
            '%06d' % rnd,
            row[1],
            status_groups[row[0]],
        ) for idx, (row, rnd) in enumerate(zip(rows, numbers))]
        rnd.shuffle(tbl)
        write_excel(args.xlsx_file, tbl, headers=headers)


@options([], requires_db=True)
def action_cleanup_requestlog(args, config, db, wdb):
    """ Remove unneeded requests, or ones we created ourselves """

    try:
        start_date = parse_date(config['startdate'])
        end_date = parse_date(config['enddate'])
    except KeyError as ke:
        raise KeyError('Missing key %s in configuration' % ke.args[0])

    wdb.execute(
        '''UPDATE requestlog2 SET deleted=1
            WHERE access_time < %s
                  OR access_time > %s''',
        (start_date, end_date))
    wdb.commit()
    print('Deleted %d rows due to date constraints' % wdb.affected_rows())

    wdb.execute(
        '''UPDATE requestlog2 SET deleted=1
            WHERE user_agent RLIKE 'GoogleBot|Pingdom|ApacheBench|bingbot|YandexBot|SISTRIX Crawler'
    ''')
    wdb.commit()
    print('Deleted %d rows due to UA constraints' % wdb.affected_rows())

    wdb.execute(
        '''CREATE OR REPLACE VIEW requestlog3 AS
            SELECT * FROM requestlog2 WHERE NOT deleted''')
    wdb.commit()


@options([
    Option(
        '--summarize',
        dest='summarize',
        help='Group into browser versions',
        action='store_true')
])
def action_list_uas(args):
    """ List user agent prevalences """

    config = read_config(args)
    with DBConnection(config) as db:
        db.execute('''SELECT user_agent, COUNT(*) as count
            FROM requestlog3 GROUP BY user_agent''')
        uastats_raw = list(db)

    def summarize(ua):
        if 'Android' in ua:
            return 'Android'
        elif 'iPhone' in ua:
            return 'iPhone'
        elif 'iPad' in ua:
            return 'iPad'
        elif 'Opera/' in ua:
            return 'Opera'
        elif 'Firefox/' in ua or 'Iceweasel/' in ua:
            return 'Firefox'
        elif 'Chromium/' in ua or 'Chrome/' in ua:
            return 'Chrome'
        elif 'MSIE ' in ua:
            return 'IE'
        elif 'Konqueror/' in ua:
            return 'Konqueror'
        elif 'Safari/' in ua:
            return 'Safari'
        elif ua.startswith('Java/'):
            return 'java'
        else:
            return ua

    uastats = collections.Counter()
    if args.summarize:
        for ua, cnt in uastats_raw:
            uastats[summarize(ua)] += cnt
    else:
        uastats.update(uastats_raw)

    for ua, cnt in uastats.most_common():
        print('%7d %s' % (cnt, ua))




@options([
    Option(
        '--timeout',
        dest='timeout',
        help='Timeout in seconds',
        type=int,
        default=600)
], requires_db=True)
def action_assign_requestlog_sessions(args, config, db, wdb):
    bar = TableSizeProgressBar(db, 'requestlog3', 'Assigning sessions')

    wdb.execute('''DROP TABLE IF EXISTS analysis_session''')
    wdb.execute('''CREATE TABLE analysis_session (
        id int PRIMARY KEY auto_increment,
        last_update_timestamp int
    )''')

    wdb.execute('''DROP TABLE IF EXISTS analysis_session_requests''')
    wdb.execute('''CREATE TABLE analysis_session_requests (
        session_id int,
        request_id int
    )''')

    def write_session(s):
        wdb.execute(
            '''INSERT INTO analysis_session
                SET last_update_timestamp=%s''', (s.time,))
        session_id = db.lastrowid
        assert session_id is not None
        wdb.executemany(
            '''INSERT INTO analysis_session_requests
                SET session_id=%s, request_id=%s''',
            [(session_id, rid) for rid in s.requests])
        return session_id

    class Session(object):
        __slots__ = 'requests', 'time'

        def __init__(self):
            self.requests = []
            self.time = None

    last_id = -1
    db.execute(
        '''SELECT id, UNIX_TIMESTAMP(access_time) as time, ip_address, user_agent
            FROM requestlog3 ORDER BY access_time ASC''')

    # key is the tuple (ip_address, user_agent)
    # value is a python  of (request_id, time) tuples
    sessions = collections.defaultdict(Session)
    STEP = 10000
    atime = None
    for idx, req in enumerate(db):
        if idx % STEP == 1:
            to_del = []
            for key, s in sessions.items():
                if s.time + args.timeout < atime:
                    last_id = write_session(s)
                    to_del.append(key)
            for key in to_del:
                del sessions[key]
        bar.next()
        request_id, atime, ip, ua = req
        key = (ip, ua)
        s = sessions[key]
        if s.time is not None and s.time + args.timeout < atime:
            last_id = write_session(s)
            s = Session()
            sessions[key] = s
        s.requests.append(request_id)
        s.time = atime

    for s in sessions.values():
        last_id = write_session(s)

    print(
        'Assigned %d sessions (timeout: %d)' %
        (last_id, args.timeout))


@options([], requires_db=True)
def action_annotate_requests(args, config, db, wdb):
    """ Filter out the interesting requests to HTML pages and copy all the
        information we got with them (for example duration) into one row"""

    bar = TableSizeProgressBar(
        db, 'requestlog3',
        'Collecting request information')

    wdb.execute('''DROP TABLE IF EXISTS analysis_request_annotations''')
    wdb.execute('''CREATE TABLE analysis_request_annotations (
        id int PRIMARY KEY auto_increment,
        request_id int,
        user_sid varchar(64),
        duration int,
        detail_json TEXT,
        INDEX (request_id),
        INDEX (user_sid)
    )''')

    class RequestInfo(object):
        __slots__ = 'access_time', 'latest_update', 'user_sid'

        def __init__(self, access_time, user_sid):
            self.access_time = access_time
            self.user_sid = user_sid
            self.latest_update = None

    def write_request(key, ri):
        ip, user_agent, request_url = key
        wdb.execute(
            '''INSERT INTO analysis_request_annotations
                SET request_id=%s, user_sid=%s
            ''', (request_id, ri.user_sid))

    # Key: (ip, user_agent, request_url), value: RequestInfo
    requests = {}

    is_stats = re.compile(r'/+(?:i/[^/]+/)?stats/')
    is_static = re.compile(r'''(?x)
        /favicon\.ico|
        /images/|
        /fanstatic/|
        /stylesheets/|
        /robots\.txt|
        /javascripts|
        # Technically not static, but very close
        /admin|
        /i/[^/]+/instance/[^/]+/settings
    ''')

    db.execute(
        '''SELECT id, UNIX_TIMESTAMP(access_time) as atime,
                  ip_address, user_agent, request_url, cookies
            FROM requestlog3 ORDER BY access_time ASC''')
    for req in db:
        bar.next()
        request_id, atime, ip, user_agent, request_url, cookies = req
        if is_stats.match(request_url):
            continue  # Skip for now
        elif is_static.match(request_url):
            continue  # Skip
        key = (ip, user_agent, request_url)
        cur = requests.get(key)
        if cur is not None:
            write_request(key, cur)
        user = extract_user_from_cookies(cookies, None)
        requests[key] = RequestInfo(atime, user)
    bar.finish()

    print('Writing out %d requests ...' % len(requests))
    for key, ri in requests.items():
        write_request(key, ri)

    wdb.execute('''CREATE OR REPLACE VIEW requestlog4 AS
        SELECT requestlog3.*,
            analysis_request_annotations.user_sid as user_sid,
            analysis_request_annotations.duration as duration,
            analysis_request_annotations.detail_json as detail_json
        FROM requestlog3, analysis_request_annotations
        WHERE requestlog3.id = analysis_request_annotations.request_id
    ''')


def main():
    parser = argparse.ArgumentParser(description='Analyze adhocracy logs')

    common_options = argparse.ArgumentParser(add_help=False)
    common_options.add_argument('files', nargs='*',
                                help='Files to read from')
    common_options.add_argument(
        '--discardfile', dest='discardfile',
        metavar='FILE', help='Store unmatching lines here')
    common_options.add_argument(
        '--config', dest='config_filename',
        metavar='FILE', help='Configuration file', default='.config.json')

    subparsers = parser.add_subparsers(
        title='action', help='What to do', dest='action')
    all_actions = [a for name, a in sorted(globals().items())
                   if name.startswith('action_')]
    for a in all_actions:
        _, e, action_name = a.__name__.partition('action_')
        assert e
        help = a.__doc__.strip() if a.__doc__ else None
        sp = subparsers.add_parser(
            action_name,
            help=help, parents=[common_options])
        for o in a.option_list:
            sp.add_argument(o.name, **o.kwargs)

    args = parser.parse_args()
    if not args.action:
        parser.error(u'No action specified')

    action = globals().get('action_' + args.action)
    action_id = 'action_' + args.action + '_'
    params = {n[len(action_id):]: getattr(args, n)
              for n in dir(args) if n.startswith(action_id)}
    action(args, **params)
