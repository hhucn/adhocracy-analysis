#import collections
import csv
import io
import random
#import sys

#from .sources import (
#    get_all_actions,
#)
from .util import (
    gen_random_numbers,
    Option,
    options,
    # parse_date,
    # timestamp_str,
    write_excel,
)


#@options([], requires_db=True)
# def action_dennis_daily_stats(args, config, db, wdb):
#     DAY_SECONDS = 24 * 60 * 60
#     start_ts = parse_date(config['startdate'])
#     end_ts = parse_date(config['enddate'])
#     all_days = [
#         timestamp_str(ts + DAY_SECONDS / 2)
#         for ts in range(start_ts, end_ts, DAY_SECONDS)]

#     def collect_stats(data):
#         sets = collections.defaultdict(set)
#         for atime, user in data:
#             day_str = timestamp_str(atime)
#             sets[day_str].add(user)
#         counts = {}
#         for d in all_days:
#             counts[d] = len(sets[d])
#         return counts

#     actions = get_all_actions(config, db)

#     res_dict = {mname: collect_stats(mrs) for mname, mrs in matching_requests.items()}
#     for a in action:
#         assert any(res_dict[mname].values()), 'Empty %s' % mname

#     res = [[d] + [res_dict[mname][d] for mname, _ in METRICS] for d in all_days]
#     csvo = csv.writer(sys.stdout)
#     csvo.writerows(res)


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


@options([
    Option('--input-file', dest='input_file', metavar='FILENAME',
           help='Name of the CSV file to read'),
    Option('--assoc-file', dest='assoc_file', metavar='FILENAME',
           help='Name of the association file (CSV format)'),
    Option('--poll-file', dest='poll_file', metavar='FILENAME',
           help='Name of the (tobias-formatted) file of the poll results'),
    Option('--output-file', dest='output_file', metavar='FILENAME',
           help='Name of the XLSX file to write'),
], requires_db=True)
def action_discher_filltable(args, config, db, wdb):
    if not args.input_file:
        raise ValueError('Missing --input-file !')
    if not args.assoc_file:
        raise ValueError('Missing --assoc-file !')
    if not args.poll_file:
        raise ValueError('Missing --poll-file !')
    if not args.output_file:
        raise ValueError('Missing --output-file !')

    name_to_uid = {}
    db.execute('''
        SELECT id, display_name FROM user WHERE id != 1
    ''')
    for row in db:
        name_to_uid[str(row[1])] = str(row[0])

    anonid_to_uid = {}
    with io.open(args.assoc_file, encoding='utf-8') as assocf:
        assoc_reader = csv.reader(assocf)
        for line in list(assoc_reader)[1:]:
            anonid = line[0]
            display_name = line[1]
            anonid_to_uid[anonid] = name_to_uid[display_name]

    # Read in polls
    poll_results = {}  # uid -> poll result
    with io.open(args.poll_file, encoding='utf-8') as pollf:
        poll_reader = csv.reader(pollf)
        poll_iter = iter(poll_reader)
        poll_headers_raw = next(poll_iter)
        poll_headers = poll_headers_raw[:1] + poll_headers_raw[2:]
        for poll_row in poll_iter:
            uid = poll_row[1]
            poll_results[uid] = poll_row[:1] + poll_row[2:]

    out = []
    with open(args.input_file, encoding='utf-8') as inputf:
        input_reader = csv.reader(inputf)
        input_iter = iter(input_reader)
        input_headers = next(input_iter)
        for input_row in input_iter:
            anonid = input_row[0]
            uid = anonid_to_uid[anonid]
            if uid in poll_results:
                out.append(input_row + poll_results[uid])
            else:
                out.append(input_row + ([''] * len(poll_headers)))

    headers = input_headers + poll_headers
    write_excel(args.output_file, out, headers)
