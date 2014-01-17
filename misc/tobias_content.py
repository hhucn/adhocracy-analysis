#!/usr/bin/env python3

from __future__ import unicode_literals

import argparse
import csv
import io
import json
import sys


class SameWidthWriter(object):
    def __init__(self, real_writer):
        self.real_writer = real_writer
        self.rows = []

    def __call__(self, row):
        assert isinstance(row, list)
        self.rows.append(row)

    def finalize(self):
        width = max(len(r) for r in self.rows)
        self.real_writer.writerows(self.rows)


def print_comment(comment, writefunc, indent=2):
    row = [comment['creator']] + [''] * indent + [comment['text']]
    writefunc(row)
    for c in comment['comments'].values():
        print_comment(c, writefunc, indent + 1)


def main():
    parser = argparse.ArgumentParser(description='Output CSV of all content')
    parser.add_argument('filename', metavar='FILE', help='input filename (JSON)')

    args = parser.parse_args()

    with io.open(args.filename, 'r', encoding='utf-8') as inf:
        d = json.load(inf)

    writer = SameWidthWriter(csv.writer(sys.stdout))
    instances = d['instance']
    for i in instances.values():
        writer(['(Instanz)', i['label']])
        for p in i['proposals'].values():
            row = [p['creator'], '', p['title'], p['description']]
            writer(row)
            for c in p['comments'].values():
                print_comment(c, writer)

    writer.finalize()


if __name__ == '__main__':
    main()