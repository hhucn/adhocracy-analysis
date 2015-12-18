from __future__ import unicode_literals

import collections

from .util import (
    options,
    Option,
    TableSizeProgressBar,
    GeoDb,
    write_data,
)


@options([
    Option(
        '--timeout',
        dest='timeout',
        help='Timeout in seconds',
        type=int,
        default=60 * 60)
], requires_db=True)
def action_assign_requestlog_sessions(args, config, db, wdb):
    bar = TableSizeProgressBar(
        db, 'analysis_requestlog_undeleted', 'Assigning sessions')

    wdb.recreate_table('analysis_session', '''
        id int PRIMARY KEY auto_increment,
        tracking_cookie text,
        first_update_timestamp int,
        last_update_timestamp int
    ''')

    wdb.recreate_table('analysis_session_requests', '''
        session_id int,
        request_id int
    ''')

    def get_session(cookies):
        tokens = cookies.split(';')
        for token in tokens:
            if token.find('user_tracking') != -1:
                _,tracking = token.split('=',1)
                return tracking
        return None;
        
    def write_session(s):
        wdb.execute(
            '''INSERT INTO analysis_session
                SET last_update_timestamp=%s,
                first_update_timestamp=%s,
                tracking_cookie=%s''', (s.time, s.first_time, s.tracking_cookie))
        session_id = wdb.lastrowid
        assert session_id is not None
        wdb.executemany(
            '''INSERT INTO analysis_session_requests
                SET session_id=%s, request_id=%s''',
            [(session_id, rid) for rid in s.requests])
        return session_id

    class Session(object):
        __slots__ = 'tracking_cookie', 'requests', 'time', 'first_time'

        def __init__(self):
            self.tracking_cookie = None
            self.requests = []
            self.time = None
            self.first_time = None

    db.execute(
        '''SELECT
                id,
                access_time,
                ip_address,
                user_agent,
                cookies
            FROM analysis_requestlog_undeleted ORDER BY access_time ASC''')

    nPos = 0
    nNeg = 0
    # sessions key is the apache cookie
    # sessions value is a python tuple of (request_id, time, first_time)
    sessions = collections.defaultdict(Session)
    for idx, req in enumerate(db):
        bar.next()
        request_id, atime, ip, ua, cookies = req
        assert atime != 0
        key = get_session(cookies)
        if key==None:
            nNeg += 1
            continue # skip requests without tracking cookie
        else:
            nPos += 1
            s = sessions[key]
            if s.first_time is None:
                s.first_time = atime
                s.tracking_cookie = key
            if s.time is not None and s.time + args.timeout < atime:
                # timeout: write old session to DB and setup new session
                last_id = write_session(s)
                del sessions[key]
                s = sessions[key]
                s.first_time = atime
                s.tracking_cookie = key
            s.requests.append(request_id)
            s.time = atime
    
    for s in sessions.values():
        last_id = write_session(s)

    # How long was each session (at least)?
    wdb.execute('''CREATE OR REPLACE VIEW analysis_session_length AS (
        SELECT
            analysis_session.id AS session_id,
            (last_update_timestamp - first_update_timestamp) AS session_length
        FROM analysis_session
    );''')
    wdb.commit()

    print(
        '\nAssigned %d sessions (timeout: %d)' %
        (last_id, args.timeout))
        
    percentNoCookie = 100 * nNeg/(nNeg+nPos)
    print('Number of requests without tracking cookie: %d (%f%%)' % (nNeg, percentNoCookie))


@options([], requires_db=True)
def action_session_user_stats(args, config, db, wdb):
    """ Calculate some simple statistics about users of sessions """

    wdb.execute('''CREATE OR REPLACE VIEW analysis_session_users AS
        (SELECT DISTINCT
            analysis_session_requests.session_id as session_id,
            analysis_requestlog_combined.user_sid as user_sid
            FROM analysis_requestlog_combined, analysis_session_requests
            WHERE analysis_requestlog_combined.id = analysis_session_requests.request_id
        )
    ''')
    wdb.commit()

    # How many sessions did each user have?
    wdb.execute('''CREATE OR REPLACE VIEW analysis_session_count_per_user AS (
        SELECT
            analysis_session_users.user_sid,
            count(analysis_session_users.session_id) as session_count
        FROM analysis_session_users, user
        WHERE analysis_session_users.user_sid = user.user_name
        GROUP BY analysis_session_users.user_sid
    );''')
    wdb.commit()

    user_ids = db.simple_query('SELECT user_sid FROM analysis_session_users')
    sessions_per_user = collections.Counter(user_ids)
    sessions_per_user['anonymous'] = sessions_per_user[None]
    del sessions_per_user[None]

    write_data('user_session_counts', {
        'data': dict(sessions_per_user.most_common()),
    })
    reverse_counts = collections.Counter(
        sessions_per_user.values()).most_common()
    write_data('user_session_counts_reverse', {
        'data': list(reverse_counts),
    })


@options()
def action_session_locations(args, config, db, wdb):
    gd = GeoDb()
    print(gd.record_by_addr('134.99.1.1'))
