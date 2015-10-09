import calendar
import collections
import io
import json
from backports import lzma
import re
import sys

from . import util
from .util import NoProgress
from .filters import filter_config_dates


Request = collections.namedtuple(
    'Request',
    ('time', 'ip', 'method', 'path', 'cookies', 'referer', 'user_agent',
     'username'))

User = collections.namedtuple(
    'User',
    ('name', 'email', 'badges')
)

Vote = collections.namedtuple(
    'Vote',
    ('id', 'subject', 'time', 'orientation', 'user')
)

Comment = collections.namedtuple(
    'Comment',
    ('id', 'time', 'user')
)

Proposal = collections.namedtuple(
    'Proposal',
    ('id', 'time', 'user')
)


def _default_discard(line):
    raise ValueError('Line %r does not match pattern' % (line))


def read_userdb(fn):
    with open(fn, 'r', encoding='utf-8') as jsonf:
        data = json.load(jsonf)

    assert data['metadata']['adhocracy_options']['include_user']
    assert data['metadata']['adhocracy_options']['include_badge']
    return {
        udata['user_name']: User(
            udata['user_name'], udata['email'], udata['badges'])
        for udata in data['user'].values()
    }


def read_requestlog(stream, *args, **kwargs):
    firstbytes = stream.read(40960)
    stream.seek(0)
    if firstbytes[:5] == b'\xfd\x37\x7a\x58\x5a':
        for r in _read_requestlog_lzma(stream):
            yield r
        return
    elif firstbytes[:1] in b'{[':
        raise NotImplementedError('JSON')
    else:
        try:
            format = _detect_apache_format(firstbytes)
        except KeyError:
            pass
        else:
            for r in _read_apache_log(stream, format, *args, **kwargs):
                yield r
            return

    raise NotImplementedError('Unrecognized input format')


def _read_requestlog_lzma(stream):
    with lzma.open(stream) as s:
        for r in read_requestlog(s):
            yield r


def _detect_apache_format(firstbytes):
    FORMATS = (
        r'''(?x)^
            (?P<ip>[0-9a-f:.]+(%[0-9a-f]{,3})?)\s+
            \[(?P<datestr>[^\]]+)\]\s+
            "(?P<reqline>[^"\\]+
                (?P<reqline_escaped>(?:\\")(?:\\"|[^"\\])+)?)"\s
            (?P<ip_>[0-9a-f:.]+(%[0-9a-f]{,3})?)\s+
            (?P<answer_code>[0-9]+)\s+
            (?P<http_proto>[^"]+)\s+
            "(?P<user_agent>[^\"]*)"\s+
            "(?P<cookie>[^"]*)"\s+
            "(?P<cookie_>[^"]*)"
            $
        ''',
    )

    for f in FORMATS:
        firstline = firstbytes.decode('utf-8', 'replace').partition('\n')[0]
        if re.match(f, firstline):
            return f
    raise KeyError('Does not match any known apache format')


def _read_apache_log(stream, format, discard=_default_discard,
                     progressclass=NoProgress):
    progress = progressclass(stream)

    month_names = dict((v, k) for k, v in enumerate(calendar.month_abbr))

    def calc_timezone_offset(tzstr):
        m = re.match(
            r'^(?P<sign>[+-])(?P<hours>[0-9]{2})(?P<minutes>[0-9]{2})$', tzstr)
        sgn = -1 if m.group('sign') == '-' else 1
        mins = sgn * (int(m.group('hours')) * 60 + int(m.group('minutes')))
        return mins * 60
    tz_cache = util.keydefaultdict(calc_timezone_offset)

    ts = io.TextIOWrapper(stream, 'utf-8', errors='strict')
    rex = re.compile(format)
    user_rex = re.compile(
        r'^[0-9a-f]{40}(?P<username>[a-z_]+)!userid_type:unicode$')
    time_rex = re.compile(r'''(?x)^
        ([0-9]{2})/   # day
        ([A-Za-z0-9]{1,})/ # month
        ([0-9]{4}):   # year
        ([0-9]{2}):   # hour
        ([0-9]{2}):   # minute
        ([0-9]{2})[ ] # second
        ([+-][0-9]{2}[0-9]{2}) # timezone
        ''')
    reqline_rex = re.compile(
        r'(?P<requestmethod>[A-Z]+)\s(?P<path>[^"]+)\sHTTP/[0-9.]+')
    for line in ts:
        m = rex.match(line)
        if not m:
            discard(line)
            continue

        reqline = m.group('reqline')
        if reqline == '-':
            continue  # Internal request

        if m.group('reqline_escaped'):
            reqline = reqline.replace('\\"', '"')

        line_m = reqline_rex.match(reqline)
        if not line_m:
            continue  # Random crap

        # Parse time
        time_m = time_rex.match(m.group('datestr'))
        assert time_m, m.group('datestr')
        rtime = calendar.timegm((
            int(time_m.group(3)),
            month_names[time_m.group(2)],
            int(time_m.group(1)),
            int(time_m.group(4)),
            int(time_m.group(5)),
            int(time_m.group(6))
        ))
        tzstr = time_m.group(7)
        rtime -= tz_cache[tzstr]

        user_m = user_rex.match(m.group('cookie'))
        if user_m:
            username = user_m.group('username')
        else:
            username = None

        req = Request(rtime, m.group('ip'), line_m.group('requestmethod'),
                      line_m.group('path'),
                      m.group('cookie'), '(no referer)', m.group('user_agent'),
                      username)
        progress.update()
        yield req

    progress.finish()


def get_votes_from_db(db):
    db.execute(
        '''SELECT
            vote.id, poll.subject, UNIX_TIMESTAMP(vote.create_time),
            vote.orientation, user.user_name
            FROM vote, poll, user
            WHERE vote.poll_id = poll.id and vote.user_id = user.id''')
    for row in db:
        yield Vote(*row)


def get_proposals_from_db(db):
    db.execute(
        '''SELECT
            proposal.id, UNIX_TIMESTAMP(delegateable.access_time), user.user_name
            FROM proposal, delegateable, user
            WHERE proposal.id = delegateable.id and delegateable.creator_id = user.id
                and delegateable.delete_time IS NULL''')
    for row in db:
        yield Proposal(*row)


def get_comments_from_db(db):
    db.execute(
        '''SELECT
            comment.id, UNIX_TIMESTAMP(comment.create_time), user.user_name
            FROM comment, delegateable, user
            WHERE comment.id = delegateable.id and comment.creator_id = user.id
                and comment.delete_time IS NULL''')
    for row in db:
        yield Comment(*row)


Action = collections.namedtuple('Action', ['key', 'rl_value', 'db_value'])


def get_all_actions(config, db):
    METRICS = [
        ('logged_in', lambda row: True, lambda *args: None),
        (
            'vote',
            lambda row: '/rate' in row[2],
            get_votes_from_db
        ),
        (
            'comment',
            lambda row: row[2].endswith('/comment'),
            get_comments_from_db
        ),
        (
            'proposal',
            lambda row: row[2].endswith('/proposal'),
            get_proposals_from_db
        ),
    ]

    # make a list of (time, user) for each action
    db.execute(
        '''SELECT access_time, user_sid, request_url, method
        FROM requestlog4
        WHERE user_sid IS NOT NULL AND user_sid != 'admin'
        ORDER BY access_time''')
    all_requests = list(db)

    matching_requests = dict(
        (mname,
         [(row[0], row[1]) for row in all_requests if mfunc(row)])
        for mname, mfunc, _ in METRICS)

    return [
        Action(
            mname,
            matching_requests[mname],
            list(filter_config_dates(dbfunc(db), config)),
        )
        for mname, _, dbfunc in METRICS]


def read_requestlog_all(args, **kwargs):
    if args.files:
        for fn in args.files:
            with open(fn, 'rb') as inf:
                for r in read_requestlog(inf, **kwargs):
                    yield r
    else:
        for r in read_requestlog(sys.stdin.buffer, **kwargs):
            yield r
