# encoding: utf-8

from __future__ import unicode_literals

import argparse
import collections
import json
import sys
import time


from . import sources

from .sources import (
    read_requestlog_all,
)
from .util import (
    Option,
    options,
    read_config,
)
from .dbhelpers import (
    DBConnection,
)

from . import hhu_actions
from . import actions_prepare
from . import actions_sessions
from . import actions_misc
from . import actions_ipppaper
from . import actions_tobias_export
from . import actions_plot


@options([
    Option('--format', dest='format', metavar='FORMAT',
           help='Output format ("repr", "json", or "benchmark")',
           default='repr')
])
def action_file_listrequests(args):
    """ Output all HTTP requests """

    def discard(entry):
        sys.stderr.write('discarding line %s' % line)

    src = read_requestlog_all(args, discard=discard)
    format = args.format
    if format == 'repr':
        for req in src:
            print(req)
    elif format == 'json':
        l = [req._asdict() for req in src]
        info = {
            '_format': 'requestlist',
            'requests': l,
        }
        json.dump(info, sys.stdout)
        print()  # Output a final newline
    elif format == 'benchmark':
        start_time = time.clock()
        count = sum(1 for _ in src)
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
        '--summarize',
        dest='summarize',
        help='Group into browser versions',
        action='store_true')
], requires_db=False)
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


@options(requires_db=True)
def action_list_votes(args, config, db, wdb):
    for v in sources.get_votes_from_db(db):
        print(v, )


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
    glbls = dict(globals())
    glbls.update(hhu_actions.__dict__)
    glbls.update(actions_prepare.__dict__)
    glbls.update(actions_sessions.__dict__)
    glbls.update(actions_misc.__dict__)
    glbls.update(actions_plot.__dict__)
    glbls.update(actions_ipppaper.__dict__)
    glbls.update(actions_tobias_export.__dict__)
    all_actions = [a for name, a in sorted(glbls.items())
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
        parser.error('No action specified')

    action = glbls['action_' + args.action]
    action_id = 'action_' + args.action + '_'
    params = {n[len(action_id):]: getattr(args, n)
              for n in dir(args) if n.startswith(action_id)}
    action(args, **params)
