import collections
import datetime
import io
import json
import lzma
import re
import time


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
            "(?:(?P<internal_request>-)|
                (?P<requestmethod>[A-Z]+)\s(?P<requestline>\S+)
                    (\sHTTP/[0-9.]+)?|
                (?P<random_crap>[\\x0-9]+)
            )"\s
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
    ts = io.TextIOWrapper(stream, 'utf-8', errors='strict')
    rex = re.compile(format)
    user_rex = re.compile(
        r'^[0-9a-f]{40}(?P<username>[a-z_]+)!userid_type:unicode$')
    for line in ts:
        m = rex.match(line)
        if not m:
            raise ValueError('Line %r does not match pattern %s' %
                             (line, format))

        if m.group('internal_request') or m.group('random_crap'):
            continue

        time_obj = datetime.datetime.strptime(m.group('datestr'),
                                              '%d/%b/%Y:%H:%M:%S %z')
        rtime = time.mktime(time_obj.timetuple())
        user_m = user_rex.match(m.group('cookie'))
        if user_m:
            username = user_m.group('username')
        else:
            username = None

        req = Request(rtime, m.group('ip'), m.group('requestmethod'),
                      m.group('requestline'),
                      'TODO: COOKIES', 'TODO: referer', m.group('user_agent'),
                      username)
        yield req
