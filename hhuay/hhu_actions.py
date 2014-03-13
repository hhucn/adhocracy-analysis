import collections
import csv
import sys

from .sources import get_votes_from_db
from .util import (
    options,
    parse_date,
    timestamp_str,
)


@options([], requires_db=True)
def action_dennis_daily_stats(args, config, db, wdb):
    # make a list of (time, user) for each action
    db.execute(
        '''SELECT access_time, user_sid, request_url, method
        FROM requestlog4
        WHERE user_sid IS NOT NULL AND user_sid != 'admin'
        ORDER BY access_time''')
    all_requests = list(db)

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
        ('voted', lambda row: 'rate' in row[2]),
        ('commented', lambda row: row[2].endswith('/comment')),
        ('proposed', lambda row: row[2].endswith('/proposal')),
    ]

    # TODO check retrieval

    res_dict = {
        mname: collect_stats(
            [(row[0], row[1]) for row in all_requests if mfunc(row)]
        )
        for mname, mfunc in METRICS}
    for mname, _ in METRICS:
        assert any(res_dict[mname].values()), 'Empty %s' % mname
    res = [[d] + [res_dict[mname][d] for mname, _ in METRICS] for d in all_days]
    csvo = csv.writer(sys.stdout)
    csvo.writerows(res)

