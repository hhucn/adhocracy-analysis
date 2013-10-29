import calendar
import collections
import datetime
import io
import json
import lzma
import re
import time

from . import util

Request = collections.namedtuple(
    'Request',
    ('time', 'ip', 'method', 'path', 'cookies', 'referer', 'user_agent',
     'username'))


User = collections.namedtuple(
    'User',
    ('name', 'email', 'badges')
)


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


def read_requestlog(stream):
    firstbytes = stream.read(40960)
    stream.seek(0)
    if firstbytes[:5] == b'\xfd\x37\x7a\x58\x5a':
        yield from _read_requestlog_lzma(stream)
        return
    elif firstbytes[:1] in b'{[':
        TODO_JSON
    else:
        try:
            format = _detect_apache_format(firstbytes)
        except KeyError:
            pass
        else:
            yield from _read_apache_log(stream, format)
            return

    raise NotImplementedError('Unrecognized input format')


def _read_requestlog_lzma(stream):
    with lzma.open(stream) as s:
        yield from read_requestlog(s)


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


def _read_apache_log(stream, format):
    month_names = dict((v, k) for k, v in enumerate(calendar.month_abbr))

    def calc_timezone_offset(tzstr):
        m = re.match(
            r'^(?P<sign>[+-])(?P<hours>[0-9]{2})(?P<minutes>[0-9]{2})$', tzstr)
        sgn = -1 if m.group('sign') == '-' else 1
        res = sgn * (int(m.group('hours')) * 60 + int(m.group('minutes')))
        return res
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
            raise ValueError('Line %r does not match pattern %r' %
                             (line, format))

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
        rtime += tz_cache[tzstr]
        rtime = 42

        user_m = user_rex.match(m.group('cookie'))
        if user_m:
            username = user_m.group('username')
        else:
            username = None

        req = Request(rtime, m.group('ip'), line_m.group('requestmethod'),
                      line_m.group('path'),
                      'TODO: COOKIES', 'TODO: referer', m.group('user_agent'),
                      username)
        yield req
