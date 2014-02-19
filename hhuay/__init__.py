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
from .util import FileProgress, DBConnection


def read_requestlog_all(args, **kwargs):
    if args.files:
        for fn in args.files:
            with open(fn, 'rb') as inf:
                yield from sources.read_requestlog(inf, **kwargs)
    else:
        yield from sources.read_requestlog(sys.stdin.buffer, **kwargs)


def action_listrequests(args, format='repr'):
    """ Output all HTTP requests """

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


def action_actionstats(args, userdb_filename=None):
    """ Display how many users did some actions, like logging in /
        posting something"""

    if not userdb_filename:
        raise ValueError('Extended analysis requires a user database')

    all_users = sources.read_userdb(userdb_filename)
    total_count = len(all_users)

    def print_cmp(txt, part, whole=total_count):
        print('%s: %d / %d (%d %%)' %
              (txt, part, whole, round(part / whole * 100)))

    welcome_rex = re.compile(r'/welcome/(?P<user>[^/]+)/')
    visited_rex = re.compile(r'^/i/')
    comment_rex = re.compile(r'^/i/[a-z]+/comment')

    clicked = collections.Counter()
    visited = collections.Counter()
    read = collections.Counter()
    voted = collections.Counter()
    commented = collections.Counter()

    for req in read_requestlog_all(args):
        m = welcome_rex.match(req.path)
        if m:
            clicked[m.group('user')] += 1

        m = visited_rex.match(req.path)
        if m and req.username:
            visited[req.username] += 1

        if '/proposal/' in req.path and req.username:
            read[req.username] += 1

        if '/poll/' in req.path and req.username:
            voted[req.username] += 1

        if (req.method == 'POST' and req.username
                and comment_rex.match(req.path)):
            commented[req.username] += 1

    clicked_count = len(clicked)
    print_cmp('Clicked on link', clicked_count)
    print_cmp('Visited an instance', len(visited))
    print_cmp('Read a proposal', len(read))
    print_cmp('Voted', len(voted))
    print_cmp('Commented', len(commented))


def action_load_requestlog(args, recreate):
    """ Load requestlog into the database """

    if not args.discardfile:
        raise ValueError('Must specify a discard file!')

    with io.open(args.discardfile, 'w', encoding='utf-8') as discardf, \
            DBConnection() as db:

        def discard(line):
            discardf.write(line)

        if recreate:
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


def action_fix_ips(args):
    """ Correct the IP addresses in the database """

    if not args.discardfile:
        raise ValueError('Must specify a discard file!')

    with io.open(args.discardfile, 'w', encoding='utf-8') as discardf, \
            DBConnection() as db:
        def discard(line):
            discardf.write(line)

        # TODO requestlog2
        sql = '''SELECT id, request_url, TO_UNIXTIME(access_time),
            FROM requestlog
            ORDER BY access_time;
        '''
        result = db.execute(sql)
        # TODO why can't we simply insert all rows?
        # TODO retrieve from this, keep a buffer

        for r in read_requestlog_all(args, discard=discard):
            pass


def action_uastats(args):
    """ Output stats about the HTTP user agents in use """
    stats = collections.Counter(
        req.user_agent for req in read_requestlog_all(args)
    )
    print('%10d %s' % (count, ua) for ua, count in stats.most_common())


#def action_nametable_dischner(args):


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
    sp = subparsers.add_parser('file_listrequests',
                               help=action_listrequests.__doc__.strip(),
                               parents=[common_options])
    sp.add_argument('--format',
                    dest='action_listrequests_format',
                    metavar='FORMAT',
                    help='Output format ("repr", "json", or "benchmark")')
    subparsers.add_parser('uastats', help=action_uastats.__doc__.strip(),
                          parents=[common_options])
    sp = subparsers.add_parser('file_actionstats',
                               help=action_actionstats.__doc__.strip(),
                               parents=[common_options])
    sp.add_argument('--userdb', dest='action_actionstats_userdb_filename',
                    help='Filename of user database', metavar='FILE')

    sp = subparsers.add_parser('fix_ips',
                               help=action_fix_ips.__doc__.strip(),
                               parents=[common_options])

    sp = subparsers.add_parser('load_requestlog',
                               help=action_load_requestlog.__doc__.strip(),
                               parents=[common_options])
    sp.add_argument('--recreate', dest='action_load_requestlog_recreate',
                    help='Drop and recreate the created requestlog table',
                    action='store_true')


    args = parser.parse_args()
    if not args.action:
        parser.error(u'No action specified')

    action = globals().get('action_' + args.action)
    action_id = 'action_' + args.action + '_'
    params = {n[len(action_id):]: getattr(args, n)
              for n in dir(args) if n.startswith(action_id)}
    action(args, **params)
