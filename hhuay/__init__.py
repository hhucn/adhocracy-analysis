import argparse
import collections
import json
import re
import sys

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
        print()
    else:
        raise ValueError('Invalid list format %r' % format)


def action_actionstats(args):
    """ Display how many users did some actions, like logging in /
        posting something"""

    total_count = 1321

    def print_cmp(txt, part, whole=total_count):
        print('%s: %d / %d (%d %%)' %
              (txt, part, whole, int(round(part / whole * 100, 0))))

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
    sp.add_argument('--format', dest='action_listrequests_format',
                    help='Output format ("repr" or "json")')
    subparsers.add_parser('uastats', help=action_uastats.__doc__.strip(),
                          parents=[common_options])
    subparsers.add_parser('actionstats',
                          help=action_actionstats.__doc__.strip(),
                          parents=[common_options])

    args = parser.parse_args()
    if not args.action:
        parser.error(u'No action specified')

    action = globals().get('action_' + args.action)
    action_id = 'action_' + args.action + '_'
    params = {n[len(action_id):]: getattr(args, n)
              for n in dir(args) if n.startswith(action_id)}
    action(args, **params)
