# encoding: utf-8

from __future__ import unicode_literals

import argparse
import io
import json
import random
import sys
import time

from . import sources
from .util import (
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
        '--recreate',
        dest='action_load_requestlog_recreate',
        help='Drop and recreate the created requestlog table',
        action='store_true')
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

        result = db.execute(
            '''UPDATE requestlog2 SET deleted=1
                WHERE 0 and (access_time < FROM_UNIXTIME(%s)
                OR access_time > FROM_UNIXTIME(%s))''',
            (start_date, end_date))
        print('execed, result: %r' % result)
        print(repr(list(result).rowcount))
        # TODO remove by UA


@options([])
def action_list_uas(args):
    """ List user agent prevalences """

    config = read_config(args)
    with DBConnection(config) as db:
        # TODO query and group
        pass


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
