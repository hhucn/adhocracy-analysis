# encoding: utf-8

from __future__ import unicode_literals

import argparse
import collections
import io
import json
import random
import sys
import time

import progress.bar

from . import sources
from .util import (
    db_simple_query,
    DBConnection,
    FileProgress,
    gen_random_numbers,
    Option,
    options,
    parse_date,
    read_config,
    write_excel,
)


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


@options([
    Option(
        '--no-recreate',
        dest='action_load_requestlog_recreate',
        help='Drop and recreate the created requestlog table',
        action='store_const', default=True, const=False)
])
def action_load_requestlog(args):
    """ Load requestlog into the database """

    if not args.discardfile:
        raise ValueError('Must specify a discard file!')

    config = read_config(args)

    with io.open(args.discardfile, 'w', encoding='utf-8') as discardf, \
            DBConnection(config) as db:

        def discard(line):
            discardf.write(line)

        if args.recreate:
            db.execute('''DROP TABLE IF EXISTS requestlog2;''')
            db.execute('''CREATE TABLE requestlog2 (
                id int PRIMARY KEY auto_increment,
                access_time datetime,
                ip_address varchar(255),
                request_url text,
                cookies text,
                user_agent text,
                deleted boolean);
            ''')

        for r in read_requestlog_all(args, discard=discard,
                                     progressclass=FileProgress):
            sql = '''INSERT INTO requestlog2
                SET access_time = FROM_UNIXTIME(%s),
                    ip_address = %s,
                    request_url = %s,
                    cookies = %s,
                    user_agent = %s;
            '''
            db.execute(sql, (r.time, r.ip, r.path, r.cookies, r.user_agent))
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


@options([])
def action_cleanup_requestlog(args):
    """ Remove unneeded requests, or ones we created ourselves """

    config = read_config(args)
    with DBConnection(config) as db:
        try:
            start_date = parse_date(config['startdate'])
            end_date = parse_date(config['enddate'])
        except KeyError as ke:
            raise KeyError('Missing key %s in configuration' % ke.args[0])

        db.execute(
            '''UPDATE requestlog2 SET deleted=1
                WHERE access_time < FROM_UNIXTIME(%s)
                      OR access_time > FROM_UNIXTIME(%s)''',
            (start_date, end_date))
        print('Deleted %d rows due to date constraints' % db.affected_rows())

        db.execute(
            '''UPDATE requestlog2 SET deleted=1
                WHERE deleted=0 AND user_agent RLIKE 'GoogleBot|Pingdom|ApacheBench|bingbot|YandexBot|SISTRIX Crawler'
        ''')
        print('Deleted %d rows due to UA constraints' % db.affected_rows())

        db.execute(
            '''CREATE OR REPLACE VIEW requestlog3 AS
                SELECT * FROM requestlog2 WHERE NOT deleted''')


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
])
def action_assign_requestlog_sessions(args):
    config = read_config(args)

    with DBConnection(config) as db, DBConnection(config) as wdb:
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

        try:
            sys.stdout.write('Calculating ETA ...')
            sys.stdout.flush()
            count = db_simple_query(db, 'SELECT COUNT(*) FROM requestlog3')[0]
        finally:
            sys.stdout.write('\r\x1b[K')
            sys.stdout.flush()

        def write_session(s):
            wdb.execute(
                '''INSERT INTO analysis_session
                    SET last_update_timestamp=%s''', (s.atime,))
            session_id = db.lastrowid
            wdb.executemany(
                '''INSERT INTO analysis_session_requests
                    SET session_id=%s, request_id=%s''',
                [(session_id, rid) for rid in s.requests])
            return session_id

        class Session(object):
            __slots__ = 'requests', 'atime'

            def __init__(self):
                self.requests = []
                self.atime = None

        last_id = -1
        bar = progress.bar.Bar(
            'Assigning sessions', max=count,
            suffix='%(percent)d%% ETA %(eta)ds')
        db.execute(
            '''SELECT id, UNIX_TIMESTAMP(access_time) as time, ip_address, user_agent
                FROM requestlog3 ORDER BY access_time ASC''')
        try:
            # key is the tuple (ip_address, user_agent)
            # value is a python  of (request_id, time) tuples
            sessions = collections.defaultdict(Session)
            STEP = 10000
            for idx, req in enumerate(db):
                if idx % STEP == 1:
                    to_del = []
                    for key, s in sessions.items():
                        if s.atime is not None and s.atime + args.timeout < atime:
                            last_id = write_session(s)
                            to_del.append(key)
                    for key in to_del:
                        del sessions[key]
                    bar.next(STEP)
                request_id, atime, ip, ua = req
                key = (ip, ua)
                s = sessions[key]
                if s.atime is not None and s.atime + args.timeout < atime:
                    last_id = write_session(s)
                    s = Session()
                    sessions[key] = s
                s.requests.append(request_id)
                s.atime = atime

            for s in bar.iter(sessions.values()):
                last_id = write_session(s)
        finally:
            bar.finish()

        print(
            'Assigned %d sessions (timeout: %d)' %
            (last_id, args.timeout))


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
