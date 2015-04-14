from __future__ import unicode_literals

import collections
import functools
import itertools
import json
import pickle
import os.path
import re
import time

from .util import (
    extract_user_from_cookies,
    options,
    Option,
)
from . import xlsx

SORTORDER_MAP = {
    '1': '-create_time',
    '2': 'order.title',
    '3': '-order.proposal.controversy',
    '4': '-order.proposal.mixed',
    '5': '-order.newestcomment',
    '6': '-order.proposal.support',
}


User = collections.namedtuple(
    'User',
    ['id', 'textid', 'name', 'gender', 'badges', 'proposal_sort_order'])


def _format_timestamp(ts):
    st = time.gmtime(ts)
    return time.strftime('%Y-%m-%d %H:%M:%S', st)


class IPAnonymizer(object):
    def __init__(self):
        self._anonymized = {}
        self._all_values = set()

    def __call__(self, ip):
        if ip in self._anonymized:
            return self._anonymized[ip]
        first, second, _, _ = ip.split('.')

        for i in itertools.count(start=1):
            s = '%s.%s.256.%d' % (first, second, i)
            if s not in self._all_values:
                break

        self._anonymized[ip] = s
        self._all_values.add(s)
        return s


def _is_external(ip):
    return not (ip.startswith('134.99.') or ip.startswith('134.94.'))


def _request_counter(rex):
    re_obj = re.compile(rex)

    def count(session):
        return sum(
            1 for r in session.requests
            if '/stats/' not in r.request_url and
               re_obj.match(r.request_url) is not None)

    return count


USER_HEADER = [
    'User ID', 'Array of Badges (JSON)', 'Female',
    'StatusProf', 'StatusCoordinator', 'StatusWiMi', 'StatusNiWiMi',
    'StatusPhD', 'StatusExtern', 'StatusStudent']


def read_users(db):
    """ Return a dictionary of textids mapping to tuples
        (User object, Cell values) """

    user_filter = 'user.delete_time IS NULL AND user.id != 1'

    db.execute('''SELECT
        user.id,
        user.user_name,
        user.display_name,
        user.gender,
        user.proposal_sort_order
    FROM user
    WHERE %s
    ORDER BY id;''' % user_filter)
    users = {
        row[0]: User(row[0], row[1], row[2], row[3], set(), row[4])
        for row in db
    }

    db.execute('''SELECT
        user.id,
        badge.title
    FROM user, user_badges, badge
    WHERE %s AND
        badge.id = user_badges.badge_id AND
        user.id = user_badges.user_id;''' % user_filter)
    for row in db:
        users[row[0]].badges.add(row[1])

    user_info = {}
    for u in users.values():
        assert u.gender in ('m', 'f')
        gender_code = 0 if u.gender == 'm' else 1
        status_prof = int("Professor/in" in u.badges)
        status_coordinator = int("KoNo-Projekt" in u.badges)
        status_wi_mi = int("Mittelbau" in u.badges)
        status_ni_wi_mi = int(
            "Weitere Mitarbeiterinnen und Mitarbeiter" in u.badges)
        status_phd = int("Doktorand/in" in u.badges)
        status_extern = 'Nicht erfasst' if status_phd else 0
        status_student = int("Studierende" in u.badges)

        assert sum(
            [status_prof, status_wi_mi, status_ni_wi_mi, status_phd,
             status_student]) <= 1
        cells = [
            u.id, json.dumps(sorted(u.badges)), gender_code,
            status_prof, status_coordinator, status_wi_mi, status_ni_wi_mi,
            status_phd, status_extern, status_student]
        user_info[u.textid] = (u, cells)
    return user_info


def export_users(ws, db):
    ws.write_header(USER_HEADER)
    sorted_uis = sorted(read_users(db).values(), key=lambda ui: ui[0].id)
    sorted_rows = [ui[1] for ui in sorted_uis]
    ws.write_rows(sorted_rows)


Session = collections.namedtuple('Session', ['id', 'requests', 'user_sid'])
Request = collections.namedtuple('Request', [
    'id', 'ip', 'access_time', 'request_url', 'cookies', 'user_agent',
    'method',
    'user_sid'])


def read_sessions(args, db):
    cache_fn = os.path.join('.cache', 'sessions.pickle')
    if os.path.exists(cache_fn):
        print('Reading sessions from pickle ...')
        with open(cache_fn, 'rb') as picklef:
            return pickle.load(picklef)

    print('Reading requests from database ...')
    db.execute('''SELECT
        analysis_requestlog_undeleted.id,
        analysis_requestlog_undeleted.ip_address,
        analysis_requestlog_undeleted.access_time,
        analysis_requestlog_undeleted.request_url,
        analysis_requestlog_undeleted.cookies,
        analysis_requestlog_undeleted.user_agent,
        analysis_requestlog_undeleted.method
    FROM
        analysis_requestlog_undeleted
    WHERE
        analysis_requestlog_undeleted.access_time > 1371803905 AND
        analysis_requestlog_undeleted.access_time < 1372787899
    ORDER BY analysis_requestlog_undeleted.id
    ;''')

    print('Collecting request info ...')
    requests_ipua = collections.defaultdict(list)
    for row in db:
        (request_id, ip, access_time, request_url, cookies,
            user_agent, method) = row

        session_id = (ip, user_agent)
        user_sid = extract_user_from_cookies(cookies)
        req = Request(
            request_id, ip, access_time, request_url, cookies,
            user_agent, method, user_sid)
        requests_ipua[session_id].append(req)

    print('Assembling session info ...')
    sessions = []

    def _write_session(sessions, by_user, key, current_time, delete=True):
        if current_time != 'force':
            if key not in by_user:
                return

            session_age = current_time - by_user[key][-1].access_time
            if session_age < args.timeout:
                return

        requests = by_user[key]
        if delete:
            del by_user[key]

        try:
            user_sid = next(r.user_sid for r in requests if r.user_sid)
        except StopIteration:
            user_sid = 'anonymous'
        s = Session(len(sessions), requests, user_sid)
        sessions.append(s)

    for requests in requests_ipua.values():
        by_user = {}
        write_session = functools.partial(_write_session, sessions, by_user)

        for r in requests:
            tainted_until = None
            if '/admin/' in r.request_url:
                requests = {}
                tainted_until = r.access_time + args.timeout
                continue
            if tainted_until and r.access_time < tainted_until:
                continue

            write_session(r.user_sid, r.access_time)
            if r.user_sid not in by_user:
                # Do we have any unassigned and usable requests?
                if (r.user_sid is not None) and (None in by_user):
                    write_session(None, r.access_time)

                    if None in by_user:
                        by_user[r.user_sid] = by_user[None]
                        del by_user[None]
                    else:
                        by_user[r.user_sid] = []
                else:
                    by_user[r.user_sid] = []

            by_user[r.user_sid].append(r)

        for k in by_user:
            write_session(k, 'force', delete=False)

    sessions.sort(key=lambda s: s.requests[0].access_time)

    print('Dumping to pickle')
    with open(cache_fn, 'wb') as cache_f:
        pickle.dump(sessions, cache_f, pickle.HIGHEST_PROTOCOL)

    return sessions


Comment = collections.namedtuple(
    'Comment', ['id', 'creator_id', 'create_time', 'revision_id', 'text'])


def read_comments(db):
    db.execute('''SELECT
        comment.id,
        comment.creator_id,
        UNIX_TIMESTAMP(comment.create_time),
        revision.id,
        revision.text
    FROM comment, revision
    WHERE comment.delete_time IS NULL AND
        comment.id = revision.comment_id
    ''')

    row_by_id = {}
    for row in db:
        comment_id = row[0]
        row_by_id[comment_id] = row

    res = {}
    for row in row_by_id.values():
        user_id = row[1]
        res.setdefault(user_id, []).append(Comment(*row))
    return res


Proposal = collections.namedtuple(
    'Proposal', ['id', 'title', 'visible', 'instance'])


def read_proposals(db):
    db.execute('''SELECT
        proposal.id,
        delegateable.label,
        delegateable.delete_time,
        instance.key
    FROM proposal, delegateable, instance
    WHERE proposal.id = delegateable.id AND
          delegateable.instance_id = instance.id
    ''')
    proposals = []
    for row in db:
        proposal_id, title, delete_time, instance_key = row
        visible = 1 if delete_time is None else 0
        proposals.append(Proposal(proposal_id, title, visible, instance_key))
    return proposals


def export_proposals(ws, db):
    ws.write_header(['id', 'visible', 'instance', 'title'])
    proposals = read_proposals(db)
    ws.write_rows([[p.id, p.visible, p.instance, p.title] for p in proposals])


def export_sessions(args, ws, db):
    print('Reading database')
    proposals = read_proposals(db)
    users = read_users(db)
    all_comments = read_comments(db)

    sessions = read_sessions(args, db)
    ipa = IPAnonymizer()

    print('Processing sessions ...')

    headers = [
        'SessionId', 'AccessFrom', 'Anonymized IP Address', 'Device Type',
        'LoginFailures',
        'SessionStart_Date', 'SessionStart', 'SessionEnd_Date', 'SessionEnd',
        'SessionDuration',
        'NavigationCount', 'VotedCount', 'CommentsWritten', 'CommentsLength',
        'StandardSortOrder', 'Resorted (JSON)',
    ]
    if args.include_proposals:
        for i, p in enumerate(proposals):
            proposal_templates = ['V%d_ID', 'V%d_Name', 'V%d_Active']
            headers += [h % i for h in proposal_templates]
    headers += USER_HEADER
    ws.write_header(headers)

    login_failures = _request_counter(r'/+post_login\?_login_tries=0')
    navigation_count = _request_counter(r'/')
    vote_count = _request_counter(r'/.*/rate\.')
    proposal_sort_order_re = re.compile(r'&proposals_sort=([0-9]+)')

    for row_num, s in enumerate(sessions, start=1):
        comments = []
        sid = s.user_sid
        if sid and (sid in users):
            ui, user_rows = users[sid]
            user_row = user_rows

            if ui.id in all_comments:
                comments = [
                    c for c in all_comments[ui.id]
                    if s.requests[0].access_time <= c.create_time - 2 and
                    c.create_time <= s.requests[-1].access_time + 2
                ]
        else:
            ui = None
            user_row = ['anonymous']

        standard_sort_order = None
        if ui:
            standard_sort_order = ui.proposal_sort_order

        resorted = []
        for r in s.requests:
            m = proposal_sort_order_re.search(r.request_url)
            if m:
                resorted.append(SORTORDER_MAP[m.group(1)])

        proposals_row = []
        if args.include_proposals:
            for p in proposals:
                proposals_row.extend([
                    p.id,
                    p.title,
                    p.visible,
                ])

        row = [
            s.id,
            'external' if _is_external(s.requests[0].ip) else 'university',
            ipa(s.requests[0].ip),
            'mobile' if 'mobile' in s.requests[0].user_agent else 'regular',
            login_failures(s),
            _format_timestamp(s.requests[0].access_time),
            s.requests[0].access_time,
            _format_timestamp(s.requests[-1].access_time),
            s.requests[-1].access_time,
            s.requests[-1].access_time - s.requests[0].access_time,
            navigation_count(s),
            vote_count(s),
            len(comments),
            sum(len(c.text) for c in comments),
            standard_sort_order,
            json.dumps(resorted) if resorted else None,
        ] + proposals_row + user_row
        ws.write_row(row_num, row)


@options([Option(
    '--output',
    metavar='FILENAME',
    dest='out_fn',
    help='Output filename'
), Option(
    '--timeout',
    dest='timeout',
    help='Session timeout in seconds',
    type=int,
    default=60 * 60
), Option(
    '--include-proposals',
    dest='include_proposals',
    action='store_true',
    help='Include proposals in session table',
)])
def action_tobias_export(args, config, db, wdb):
    book = xlsx.gen_doc(args.out_fn, ['Sessions', 'Benutzer', 'Proposals'])

    export_sessions(args, book.worksheets_objs[0], db)
    export_users(book.worksheets_objs[1], db)
    export_proposals(book.worksheets_objs[2], db)

    book.close()
