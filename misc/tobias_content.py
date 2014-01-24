#!/usr/bin/env python3

from __future__ import unicode_literals


import argparse
import collections
import datetime
import io
import json
import sys

import xlsxwriter
import pytz


headers = [
    'Instanz',
    'Name Vorschlag',
    'Benutzername',
    'Statusgruppe',
    'Datum',
    'Uhrzeit',
    'pro',
    'contra',
    'Text Vorschlag',
    'Laufende Nummer',
]

Comment = collections.namedtuple('Comment', [
    'instance', 'proposal_title', 'proposal_text',
    'user', 'timestamp', 'pro',
    'contra', 'text', 'index', 'depth'])


def get_group(user, data):
    user_data = data['user'][user]
    for b in ('Professor/in', 'Mittelbau', 'Doktorand/in'):
        if b in user_data['badges']:
            return b
    return 'Sonstige'


def comment_to_row(c, data):
    dt = datetime.datetime.strptime(c.timestamp, '%Y-%m-%dT%H:%M:%S')
    utc = pytz.timezone('UTC')
    dt = dt.replace(tzinfo=utc)

    local_tz = pytz.timezone('Europe/Berlin')
    local_dt = dt.astimezone(local_tz)

    date = local_dt.date()
    time = local_dt.time()

    group = get_group(c.user, data)

    return (
        [c.instance, c.proposal_title, c.user,
            group, date, time,
            c.pro, c.contra, c.proposal_text, c.index] +
        ([''] * c.depth) + [c.text])


def render_text(t):
    return t.replace('\r', '')


def walk_comments(item, depth=1):
    sub_comments = item['comments'].values()
    sub_comments.sort(key=lambda c: c['create_time'])
    for c in sub_comments:
        yield (c, depth)
        assert c['adhocracy_type'] == 'comment'
        for sub in walk_comments(c, depth + 1):
            yield sub


def get_comment_data(data):
    for i in data['instance'].values():
        instance_title = i['label']
        for p in i['proposals'].values():
            proposal_title = p['title']
            proposal_text = render_text(p['description'])

            # Yield the proposal itself
            assert p['adhocracy_type'] == 'proposal'
            yield Comment(
                instance_title, proposal_title, proposal_text,
                p['creator'], p['create_time'],
                p['rate_pro'], p['rate_contra'], proposal_text,
                0, 0)

            for cidx, ctpl in enumerate(walk_comments(p)):
                c, depth = ctpl
                assert c['adhocracy_type'] == 'comment'
                yield Comment(
                    instance_title, proposal_title, proposal_text,
                    c['creator'], c['create_time'],
                    c['rate_pro'], c['rate_contra'], render_text(c['text']),
                    cidx + 1, depth)


def main():
    parser = argparse.ArgumentParser(description='Output CSV of all content')
    parser.add_argument('input', metavar='FILE', help='input filename (JSON)')
    parser.add_argument('output', metavar='FILE', help='output filename (xlsx)')

    args = parser.parse_args()

    workbook = xlsxwriter.Workbook(args.output)
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': 1})
    date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
    time_format = workbook.add_format({'num_format': 'HH:MM:SS'})

    with io.open(args.input, 'r', encoding='utf-8') as inf:
        d = json.load(inf)
    cd = list(get_comment_data(d))

    for i, h in enumerate(headers):
        worksheet.write(0, i, h, bold)
        worksheet.set_column(i, i, max(10, 2 * len(h)))
    max_depth = max(c.depth for c in cd)
    for i in range(max_depth):
        worksheet.write(0, len(headers) + i, 'Kommentar Ebene %d' % i, bold)

    for rowidx, c in enumerate(cd):
        row = comment_to_row(c, d)
        for colidx, cell in enumerate(row):
            if isinstance(cell, datetime.date):
                worksheet.write_datetime(rowidx + 1, colidx, cell, date_format)
            elif isinstance(cell, datetime.time):
                worksheet.write_datetime(rowidx + 1, colidx, cell, time_format)
            else:
                worksheet.write(rowidx + 1, colidx, cell)
    workbook.close()

if __name__ == '__main__':
    main()
