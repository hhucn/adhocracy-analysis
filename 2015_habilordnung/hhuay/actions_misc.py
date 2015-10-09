from __future__ import unicode_literals

from .util import (
    options,
    Option,
    parse_date,
    sql_filter,
    TableSizeProgressBar,
)


@options(requires_db=True)
def action_basicfacts(args, config, db, wdb):
    """ Output some basic information about the project """

    start_date = parse_date(config['startdate'])
    end_date = parse_date(config['enddate'])
    print('%s - %s (%r days)' % (
        start_date, end_date, (end_date - start_date) // (24 * 60 * 60)))

    user_count = db.simple_query('''
        SELECT COUNT(*) FROM user WHERE id != 1 and delete_time IS NULL''')[0]
    print('%d users' % user_count)

    request_count = db.simple_query('''
        SELECT COUNT(*) FROM analysis_requestlog_undeleted''')[0]
    print('%d requests (alltogether)' % request_count)

    nonstat_request_count = db.simple_query('''
        SELECT COUNT(analysis_requestlog_undeleted.id)
        FROM analysis_requestlog_undeleted, analysis_request_annotations
        WHERE analysis_requestlog_undeleted.id = analysis_request_annotations.id
            AND request_url NOT LIKE '%/stats/%';
    ''')[0]
    print('%d page loads' % nonstat_request_count)

    time_q = "create_time >= FROM_UNIXTIME(%d) AND create_time <= FROM_UNIXTIME(%d)" % (
        start_date, end_date)
    # 1 = admin (i.e. created by us)
    where_q = ' WHERE creator_id != 1 AND delete_time IS NULL AND ' + time_q + sql_filter('proposal', config)
    proposal_count = db.simple_query(
        'SELECT COUNT(*) FROM delegateable' + where_q + ' AND type="proposal"')[0]
    print('%d proposals' % proposal_count)

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
