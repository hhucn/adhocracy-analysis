from __future__ import unicode_literals

import re

from .util import (
    extract_user_from_cookies,
    FileProgress,
    options,
    parse_date,
    sql_filter,
    TableSizeProgressBar,
)
from .sources import get_requests_from_db

@options([], requires_db=True)
def action_load_requestlog(args, config, db, wdb):
    """ Creates analysis_requestlog database table based off requestlog table """

    wdb.drop_table('analysis_requestlog')
    wdb.execute('''CREATE TABLE analysis_requestlog (
        id int PRIMARY KEY auto_increment,
        access_time int,
        ip_address varchar(255),
        request_url text,
        cookies text,
        user_agent text,
        deleted boolean NOT NULL,
        method varchar(10));
    ''')

    # We need a second cursor due to parallel db access
    cur2 = wdb.db.cursor(buffered=True)
    
    for r in get_requests_from_db(db):
        sql = '''INSERT INTO analysis_requestlog
            SET access_time = %s,
                ip_address = %s,
                request_url = %s,
                cookies = %s,
                user_agent = %s,
                method = '',
                deleted = 0;
        '''
        cur2.execute(
            sql,
            (r.time, r.ip, r.path, r.cookies, r.user_agent))
        
    wdb.commit()


@options([], requires_db=True)
def action_cleanup_requestlog(args, config, db, wdb):
    """ Remove unneeded requests, or ones we created ourselves """

    try:
        start_date = parse_date(config['startdate'])
        end_date = parse_date(config['enddate'])
    except KeyError as ke:
        raise KeyError('Missing key %s in configuration' % ke.args[0])

    wdb.execute(
        '''UPDATE analysis_requestlog SET deleted=1
            WHERE access_time < %s
                  OR access_time > %s''',
        (start_date, end_date))
    wdb.commit()
    print('Deleted %d rows due to date constraints' % wdb.affected_rows())

    wdb.execute(
        '''UPDATE analysis_requestlog SET deleted=1
            WHERE user_agent RLIKE
            'GoogleBot|Pingdom|ApacheBench|bingbot|YandexBot|SISTRIX Crawler';
    ''')
    wdb.commit()
    print('Deleted %d rows due to UA constraints' % wdb.affected_rows())

    wdb.execute(
        '''UPDATE analysis_requestlog SET deleted=1
            WHERE cookies is NULL;
    ''')
    wdb.commit()
    print('Deleted %d rows that do not have cookies' % wdb.affected_rows())

    wdb.execute(
        '''CREATE OR REPLACE VIEW analysis_requestlog_undeleted AS
            SELECT * FROM analysis_requestlog WHERE NOT deleted''')
    wdb.commit()

@options(requires_db=True)
def action_annotate_requests(args, config, db, wdb):
    """ Filter out the interesting requests to HTML pages and copy all the
        information we got with them (for example duration) into one row"""

    bar = TableSizeProgressBar(
        db, 'analysis_requestlog_undeleted',
        'Collecting request information')

    wdb.recreate_table('analysis_request_annotations', '''
        id int PRIMARY KEY auto_increment,
        request_id int,
        user_sid varchar(64),
        duration int,
        detail_json TEXT,
        INDEX (request_id),
        INDEX (user_sid)
    ''')

    class RequestInfo(object):
        __slots__ = 'request_id', 'access_time', 'latest_update', 'user_sid'

        def __init__(self, request_id, access_time, user_sid):
            self.request_id = request_id
            self.access_time = access_time
            self.user_sid = user_sid
            self.latest_update = None

        def __str__(self):
            return '%d %s' % (self.access_time, self.user_sid)

    def write_request(key, ri):
        ip, user_agent, request_url = key
        wdb.execute(
            '''INSERT INTO analysis_request_annotations
                SET request_id=%s, user_sid=%s
            ''', (ri.request_id, ri.user_sid))

    # Key: (ip, user_agent, request_url), value: RequestInfo
    requests = {}

    is_stats = re.compile(r'/+(?:i/[^/]+/)?stats/')
    is_static = re.compile(r'''(?x)
        /favicon\.ico|
        /images/|
        /fanstatic/|
        /stylesheets/|
        /robots\.txt|
        /javascripts|
        # Technically not static, but very close
        /admin|
        /i/[^/]+/instance/[^/]+/settings
    ''')

    write_count = 0
    db.execute(
        '''SELECT id, access_time as atime,
                  ip_address, user_agent, request_url, cookies
            FROM analysis_requestlog_undeleted
            ORDER BY access_time ASC
            ''')
    for req in db:
        bar.next()
        request_id, atime, ip, user_agent, request_url, cookies = req
        if is_static.match(request_url):
            continue  # Skip
        #assert '/stats' not in request_url
        key = (ip, user_agent, request_url)
        cur = requests.get(key)
        if cur is not None:
            write_request(key, cur)
            del requests[key]
            write_count += 1
        user = extract_user_from_cookies(cookies, None)
        requests[key] = RequestInfo(request_id, atime, user)
    bar.finish()

    print('Writing out %d requests (already wrote out %d inline) ...' % (
        len(requests), write_count))
    for key, ri in requests.items():
        write_request(key, ri)

    wdb.execute('''CREATE OR REPLACE VIEW analysis_requestlog_combined AS
        SELECT analysis_requestlog_undeleted.*,
            analysis_request_annotations.user_sid as user_sid,
            analysis_request_annotations.duration as duration,
            analysis_request_annotations.detail_json as detail_json
        FROM analysis_requestlog_undeleted, analysis_request_annotations
        WHERE analysis_requestlog_undeleted.id = analysis_request_annotations.request_id
    ''')

@options()
def action_user_classification(args, config, db, wdb):
    start_date = parse_date(config['startdate'])
    end_date = parse_date(config['enddate'])

    time_q = "create_time >= FROM_UNIXTIME(%d) AND create_time <= FROM_UNIXTIME(%d)" % (
        start_date, end_date)

    # 1 = admin (i.e. created by us)
    where_q = ' WHERE creator_id != 1 AND delete_time IS NULL AND ' + time_q + sql_filter('proposal', config)
    proposal_authors = db.simple_query(
        'SELECT COUNT(delegateable.id) FROM delegateable' + where_q + ' AND type="proposal"')[0]

    where_q = ' WHERE creator_id != 1 AND delete_time IS NULL AND ' + time_q + sql_filter('comment', config)
    comment_count = db.simple_query(
        'SELECT COUNT(*) FROM comment ' + where_q)[0]
    print('%d comments' % comment_count)

    where_q = ' WHERE user_id != 1 AND ' + time_q + sql_filter('vote', config)
    raw_vote_count = db.simple_query(
        'SELECT COUNT(*) FROM vote ' + where_q)[0]
    print('%d votes' % raw_vote_count)

    where_q = ' WHERE user_id != 1 AND ' + time_q + sql_filter('vote', config)
    vote_count = db.simple_query(
        'SELECT COUNT(DISTINCT user_id, poll_id) FROM vote ' + where_q)[0]
    print('%d votings' % vote_count)
