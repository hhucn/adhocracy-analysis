#!/usr/bin/env python3

from __future__ import unicode_literals


import argparse
import collections
import io
import json
import sys

import xlsxwriter


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


def comment_to_row(c, data):
    return [
        c.instance, c.proposal_title, c.user,
        'TODO: Statusgruppe', 'TODO: DATUM', 'TODO: Uhrzeit',
        c.pro, c.contra, c.text, c.index]


def render_text(t):
    return t.replace('\r', '')


def walk_comments(item, depth=1):
    sub_comments = item['comments'].values()
    # TODO sort
    for idx, c in enumerate(sub_comments):
        yield (c, idx, depth)
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

            for c, index, depth in walk_comments(p):
                assert c['adhocracy_type'] == 'comment'
                yield Comment(
                    instance_title, proposal_title, proposal_text,
                    c['creator'], c['create_time'],
                    c['rate_pro'], c['rate_contra'], render_text(c['text']),
                    index, depth)


def main():
    parser = argparse.ArgumentParser(description='Output CSV of all content')
    parser.add_argument('input', metavar='FILE', help='input filename (JSON)')
    parser.add_argument('output', metavar='FILE', help='output filename (xlsx)')

    args = parser.parse_args()

    workbook = xlsxwriter.Workbook(args.output)
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': 1})

    with io.open(args.input, 'r', encoding='utf-8') as inf:
        d = json.load(inf)
    cd = list(get_comment_data(d))

    max_depth = max(c.depth for c in cd)
    for i, h in enumerate(headers):
        worksheet.write(0, i, h, bold)
        worksheet.set_column(i, i, max(10, 2 * len(h)))

    for rowidx, c in enumerate(cd):
        row = comment_to_row(c, d)
        for colidx, cell in enumerate(row):
            worksheet.write(rowidx + 1, colidx, cell)
    workbook.close()

if __name__ == '__main__':
    main()


# TODO regard depth
# TODO sort
# TODO render date / time
# TODO Statusgruppe