import collections
import csv
import random
import sys

from .sources import (
    get_all_actions,
)
from .util import (
    gen_random_numbers,
    Option,
    options,
    parse_date,
    timestamp_str,
    write_excel,
)


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

    actions = get_all_actions(config, db)

    for a in actions:
        sys.stderr.write('%s: %d in db, %d in requests\n' % (
            a.key, len(a.db_value), len(a.rl_value)))
    return
    res_dict = {mname: collect_stats(mrs) for mname, mrs in matching_requests.items()}
    for a in action:
        assert any(res_dict[mname].values()), 'Empty %s' % mname

    res = [[d] + [res_dict[mname][d] for mname, _ in METRICS] for d in all_days]
    csvo = csv.writer(sys.stdout)
    csvo.writerows(res)


@options([
    Option('--xlsx-file', dest='xlsx_file', metavar='FILENAME',
           help='Name of the Excel file to write')
], requires_db=True)
def action_dischner_nametable(args, config, db, wedb):
    """ Create a list of names and user IDs and write is as xlsx """

    if not args.xlsx_file:
        raise ValueError('Must specify an output file!')

    from .hhu_specific import get_status_groups

    status_groups = get_status_groups(db)
    db.execute('SELECT id, display_name FROM user where id != 1')
    rows = list(db)

    rnd = random.Random(123)
    numbers = gen_random_numbers(rnd, 0, 999999, len(rows))

    headers = ('ID', 'Name', 'Statusgruppe')
    tbl = [(
        '%06d' % rnd_id,
        row[1],
        status_groups[row[0]],
    ) for idx, (row, rnd_id) in enumerate(zip(rows, numbers))]
    rnd.shuffle(tbl)
    write_excel(args.xlsx_file, tbl, headers=headers)
