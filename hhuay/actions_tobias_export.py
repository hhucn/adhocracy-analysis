from __future__ import unicode_literals

import collections
import itertools
import json

from .util import (
    options,
    Option,
    TableSizeProgressBar,
)
from . import xlsx


User = collections.namedtuple('User', ['id', 'name', 'gender', 'badges'])


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


USER_HEADER = [
    'User ID', 'Array of Badges (JSON)', 'Female',
    'StatusProf', 'StatusCoordinator', 'StatusWiMi', 'StatusNiWiMi',
    'StatusPhD', 'StatusExtern', 'StatusStudent']


def read_users(db):
    """ Return tuples (User object, Cell values) """

    user_filter = 'user.delete_time IS NULL AND user.id != 1'

    db.execute('''SELECT
        user.id,
        user.display_name,
        user.gender
    FROM user
    WHERE %s
    ORDER BY id;''' % user_filter)
    users = {
        row[0]: User(row[0], row[1], row[2], set())
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
        user_info[u.id] = (u, cells)
    return user_info


def export_users(ws, db):
    ws.write_header(USER_HEADER)
    sorted_uis = sorted(read_users(db).values(), key=lambda ui: ui[0].id)
    sorted_rows = [ui[1] for ui in sorted_uis]
    ws.write_rows(sorted_rows)


Session = collections.namedtuple('Session', ['id', 'requests'])
Request = collections.namedtuple('Request', ['id', 'ip'])


def read_sessions(db):
    limit = 'analysis_session.id < 100'
    sessions = {}
    db.execute('''SELECT
        analysis_session.id
    FROM analysis_session
    WHERE %s
    ORDER BY id;''' % limit)
    for row in db:
        session_id, = row
        sessions[session_id] = Session(session_id, [])

    print('Running request ...')
    db.execute('''SELECT
        analysis_session.id,
        analysis_requestlog_undeleted.id,
        analysis_requestlog_undeleted.ip_address
    FROM
        analysis_session,
        analysis_session_requests,
        analysis_requestlog_undeleted
    WHERE
        %s AND
        analysis_session.id = analysis_session_requests.session_id AND
        analysis_requestlog_undeleted.id = analysis_session_requests.request_id
    ORDER BY analysis_session.id
    ;''' % limit)
    print('Collecting data ...')

    for row in db:
        session_id, request_id, ip = row
        req = Request(request_id, ip)
        sessions[session_id].requests.append(req)

    for s in sessions.values():
        assert len(s.requests) > 0, s

    return sessions


def export_sessions(ws, db):
    headers = ['SessionId', 'AccessFrom'] + USER_HEADER
    ws.write_header(headers)

    users = read_users(db)
    sessions = read_sessions(db)
    ipa = IPAnonymizer()

    sorted_sessions = sorted(sessions.values(), key=lambda s: s.id)
    for row_num, s in enumerate(sorted_sessions, start=1):
        row = [
            s.id,
            'external' if _is_external(s.requests[0].ip) else 'university',
            ipa(s.requests[0].ip),
        ]
        ws.write_row(row_num, row)


@options([Option(
    '--output',
    metavar='FILENAME',
    dest='out_fn',
    help='Output filename')
])
def action_tobias_export(args, config, db, wdb):
    book = xlsx.gen_doc(args.out_fn, ['Sessions', 'Benutzer'])

    export_sessions(book.worksheets_objs[0], db)
    export_users(book.worksheets_objs[1], db)

    book.close()
