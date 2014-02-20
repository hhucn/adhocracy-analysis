import argparse
import collections
import io
import json
import re
import sys
import time

import mysql.connector
from mysql.connector.constants import ClientFlag

from . import sources
from .util import FileProgress, DBConnection, options, Option


def read_requestlog_all(args, **kwargs):
    if args.files:
        for fn in args.files:
            with open(fn, 'rb') as inf:
                yield from sources.read_requestlog(inf, **kwargs)
    else:
        yield from sources.read_requestlog(sys.stdin.buffer, **kwargs)


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

    with io.open(args.discardfile, 'w', encoding='utf-8') as discardf, \
            DBConnection() as db:

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


    

def main():
    parser = argparse.ArgumentParser(description='Analyze adhocracy logs')

    common_options = argparse.ArgumentParser(add_help=False)
    common_options.add_argument('files', nargs='*',
                                help='Files to read from')
    common_options.add_argument(
        '--discardfile', dest='discardfile',
        metavar='FILE', help='Store unmatching lines here')
    common_options.add_argument(
        '--config', dest='configfile',
        metavar='FILE', help='Configuration file')

    subparsers = parser.add_subparsers(
        title='action', help='What to do', dest='action')
    all_actions = [a for name, a in sorted(globals().items())
                   if name.startswith('action_')]
    print(all_actions)
    for a in all_actions:
        _, e, action_name = a.__name__.partition('action_')
        assert e
        sp = subparsers.add_parser(
            action_name,
            help=a.__doc__.strip(), parents=[common_options])
        sp.add_argument('--userdb', dest='action_actionstats_userdb_filename',
            help='Filename of user database', metavar='FILE')
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
