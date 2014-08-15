from __future__ import unicode_literals

import collections

from .util import (
    options,
    Option,
    TableSizeProgressBar,
)


@options([
    Option(
        '--timeout',
        dest='timeout',
        help='Timeout in seconds',
        type=int,
        default=600)
], requires_db=True)
def action_assign_requestlog_sessions(args, config, db, wdb):
    bar = TableSizeProgressBar(
        db, 'analysis_requestlog_undeleted', 'Assigning sessions')

    wdb.execute('analysis_session', '''
        id int PRIMARY KEY auto_increment,
        last_update_timestamp int
    ''')

    wdb.recreate_table('analysis_session_requests', '''
        session_id int,
        request_id int
    ''')

    def write_session(s):
        wdb.execute(
            '''INSERT INTO analysis_session
                SET last_update_timestamp=%s''', (s.time,))
        session_id = wdb.lastrowid
        assert session_id is not None
        wdb.executemany(
            '''INSERT INTO analysis_session_requests
                SET session_id=%s, request_id=%s''',
            [(session_id, rid) for rid in s.requests])
        return session_id

    class Session(object):
        __slots__ = 'requests', 'time'

        def __init__(self):
            self.requests = []
            self.time = None

    last_id = -1
    db.execute(
        '''SELECT
                id,
                access_time,
                ip_address,
                user_agent
            FROM analysis_requestlog_undeleted ORDER BY access_time ASC''')

    # key is the tuple (ip_address, user_agent)
    # value is a python  of (request_id, time) tuples
    sessions = collections.defaultdict(Session)
    STEP = 10000
    atime = None
    for idx, req in enumerate(db):
        if idx % STEP == 1:
            to_del = []
            for key, s in sessions.items():
                if s.time + args.timeout < atime:
                    last_id = write_session(s)
                    to_del.append(key)
            for key in to_del:
                del sessions[key]
        bar.next()
        request_id, atime, ip, ua = req
        assert atime != 0
        key = (ip, ua)
        s = sessions[key]
        if s.time is not None and s.time + args.timeout < atime:
            last_id = write_session(s)
            del sessions[key]
            s = sessions[key]
        s.requests.append(request_id)
        s.time = atime

    for s in sessions.values():
        last_id = write_session(s)

    print(
        'Assigned %d sessions (timeout: %d)' %
        (last_id, args.timeout))


@options([], requires_db=True)
def action_session_stats(args, config, db, wdb):
    """ Calculate some simple statistics about sessions """

    wdb.execute('''CREATE OR REPLACE VIEW analysis_session_users AS
        (SELECT DISTINCT
            analysis_session_requests.session_id as session_id,
            analysis_requestlog_combined.user_sid as user_sid
            FROM analysis_requestlog_combined, analysis_session_requests
            WHERE analysis_requestlog_combined.id = analysis_session_requests.request_id
        )
    ''')
    wdb.commit()


    # TODO average session length

    # TODO calc users in sessions


    # TODO number of users per session (error correction)
    # TODO number of sessions per user
    # TODO calc session lengths
