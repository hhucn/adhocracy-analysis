import collections
import csv
import sys

from .sources import (
    get_comments_from_db,
    get_proposals_from_db,
    get_votes_from_db,
)
from .util import (
    options,
    parse_date,
    timestamp_str,
)
from .filters import filter_config_dates


@options([], requires_db=True)
def action_dennis_daily_stats(args, config, db, wdb):
    DAY_SECONDS = 24 * 60 * 60
    start_ts = parse_date(config['startdate'])
    end_ts = parse_date(config['enddate'])
    all_days = [
        timestamp_str(ts + DAY_SECONDS / 2)
        for ts in range(start_ts, end_ts, DAY_SECONDS)]

    def collect_stats(data):
        sets = collections.defaultdict(set)
        for atime, user in data:
            day_str = timestamp_str(atime)
            sets[day_str].add(user)
        counts = {}
        for d in all_days:
            counts[d] = len(sets[d])
        return counts

    METRICS = [
        ('logged_in', lambda row: True),
        ('vote', lambda row: '/rate' in row[2]),
        ('comment', lambda row: row[2].endswith('/comment')),
        ('proposal', lambda row: row[2].endswith('/proposal')),
    ]

    # Fetch database-recorded values
    db_values = {
        'vote': filter_config_dates(get_votes_from_db(db), config),
        'comment': filter_config_dates(get_comments_from_db(db), config),
        'proposal': filter_config_dates(get_proposals_from_db(db), config),
    }

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
        for mname, mfunc in METRICS)

    for k in sorted(db_values):
        print('%s: %d in db, %d in requests' % (k, len(list(db_values[k])), len(matching_requests[k])))
    return

    res_dict = {mname: collect_stats(mrs) for mrs in matching_requests}
    for mname, _ in METRICS:
        assert any(res_dict[mname].values()), 'Empty %s' % mname

    res = [[d] + [res_dict[mname][d] for mname, _ in METRICS] for d in all_days]
    csvo = csv.writer(sys.stdout)
    csvo.writerows(res)

