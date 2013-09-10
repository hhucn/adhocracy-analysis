import argparse
import collections
import json
import re
import sys
import time

from . import sources


def read_requestlog_all(args):
    if args.files:
        for fn in args.files:
            with open(fn, 'rb') as inf:
                yield from sources.read_requestlog(inf)
    else:
        yield from sources.read_requestlog(sys.stdin.buffer)


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


def action_uastats(args):
    """ Output stats about the HTTP user agents in use """
    stats = collections.Counter(
        req.user_agent for req in read_requestlog_all(args)
    )
    print('%10d %s' % (count, ua) for ua, count in stats.most_common())


def main():
    parser = argparse.ArgumentParser(description='Analyze adhocracy logs')

    common_options = argparse.ArgumentParser(add_help=False)
    common_options.add_argument('files', nargs='*',
                                help='Files to read from')

    subparsers = parser.add_subparsers(
        title='action', help='What to do', dest='action')
    sp = subparsers.add_parser('listrequests',
                               help=action_listrequests.__doc__.strip(),
                               parents=[common_options])
    sp.add_argument('--format',
                    dest='action_listrequests_format',
                    metavar='FORMAT',
                    help='Output format ("repr", "json", or "benchmark")')
    subparsers.add_parser('uastats', help=action_uastats.__doc__.strip(),
                          parents=[common_options])
    sp = subparsers.add_parser('actionstats',
                               help=action_actionstats.__doc__.strip(),
                               parents=[common_options])
    sp.add_argument('--userdb', dest='action_actionstats_userdb_filename',
                    help='Filename of user database', metavar='FILE')

    args = parser.parse_args()
    if not args.action:
        parser.error(u'No action specified')

    action = globals().get('action_' + args.action)
    action_id = 'action_' + args.action + '_'
    params = {n[len(action_id):]: getattr(args, n)
              for n in dir(args) if n.startswith(action_id)}
    action(args, **params)
