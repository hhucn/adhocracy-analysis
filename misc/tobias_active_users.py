#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import unicode_literals


import argparse
import collections
import datetime
import io
import json

import xlsxwriter
import pytz


headers = [
    'Name',
    #'Vorname',
    #'Nachname',
    'E-Mail',
    'Geschlecht',
    'Statusgruppe',
    'Badges'
]

def is_active(user, data):
    user_data = data['user'][user]
    for b in user_data['badges']:
        if b == 'ACTIVE':
            return True
    return False


def get_group(user, data):
    user_data = data['user'][user]
    for b in ('Professor/in / PD', 'Postdoktorand/in'):
        if b in user_data['badges']:
            return b
    return 'Andere'


def get_badges(user, data):
    user_data = data['user'][user]
    badges = ''
    for b in user_data['badges']:
        if not b.isupper() and b not in ('Professor/in / PD', 'Postdoktorand/in', 'Andere'):
            badges += b+', '
    return badges


def get_user_data(data):
    for u in data['user'].values():
        user_name = u['user_name']
        
        if is_active(user_name, data):
            user_full_name = u['display_name']
            tokens = user_full_name.split(' ', 1)
            #user_first_name = tokens[0]
            #user_last_name = tokens[1]
            user_email = u['email']
            user_gender = u['gender']
            user_group = get_group(user_name, data)
            user_badges = get_badges(user_name, data)

            yield [user_full_name,
                    #user_first_name, user_last_name,
                    user_email, user_gender,
                    user_group, user_badges]


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
    ud = list(get_user_data(d))

    for i, h in enumerate(headers):
        worksheet.write(0, i, h, bold)
        worksheet.set_column(i, i, 25)

    for rowidx, c in enumerate(ud):
        for colidx, cell in enumerate(c):
            worksheet.write(rowidx + 1, colidx, cell)
    workbook.close()

if __name__ == '__main__':
    main()
