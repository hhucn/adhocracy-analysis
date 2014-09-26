from __future__ import unicode_literals

from .util import (
    options,
    Option,
    parse_date,
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

    # TODO exclude proposals by admin and deleted

    #print('%d proposals' % proposal_count)

    # vote_count = 
    #print('%d votes' % nonstat_request_count)

    # TODO votes

    # TODO # votes
    # TODO # comments
