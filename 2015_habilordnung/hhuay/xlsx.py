from __future__ import unicode_literals

import functools


def write_header(worksheet, columns, row=0, column_offset=0):
    for i, column_name in enumerate(columns, column_offset):
        worksheet.write(row, i, column_name, worksheet._fbc_formats['header'])


def write_heading(worksheet, title, row, col):
    worksheet.write(row, col, title, worksheet._fbc_formats['heading'])


def write_heading_range(worksheet, title, row, col, width, height=1):
    import xlsxwriter.utility
    range_start = xlsxwriter.utility.xl_rowcol_to_cell(row, col)
    range_end = xlsxwriter.utility.xl_rowcol_to_cell(
        row + height - 1, col + width - 1)
    worksheet.merge_range(
        '%s:%s' % (range_start, range_end), title,
        worksheet._fbc_formats['heading_range'])


def write_cell_range(worksheet, value, row, col, width, height=1):
    import xlsxwriter.utility
    range_start = xlsxwriter.utility.xl_rowcol_to_cell(row, col)
    range_end = xlsxwriter.utility.xl_rowcol_to_cell(
        row + height - 1, col + width - 1)
    worksheet.merge_range(
        '%s:%s' % (range_start, range_end), value)


def write_row(worksheet, row_num, values, column_offset=0):
    for i, v in enumerate(values, start=column_offset):
        worksheet.write(row_num, i, v)


def write_rows(worksheet, rows, row_offset=1, column_offset=0):
    for row_num, row in enumerate(rows, start=row_offset):
        write_row(worksheet, row_num, row, column_offset=column_offset)


def gen_doc(fn, worksheet_names,
            props={
                'author': 'Philipp Hagemeister',
                'company': 'HHU Düsseldorf'}):

    import xlsxwriter

    if fn is None:
        raise ValueError('No output filename specified')

    workbook = xlsxwriter.Workbook(
        fn, {'strings_to_urls': False, 'in_memory': True})
    workbook.set_properties(props)

    fbc_formats = {
        'heading': workbook.add_format({'bold': True}),
        'heading_range': workbook.add_format(
            {'bold': True, 'align': 'center'}),
        'header': workbook.add_format({'bold': True, 'bottom': 1}),
    }

    allfuncs = {
        funcname: func
        for funcname, func in globals().items()
        if funcname.startswith('write_')
    }

    for wn in worksheet_names:
        ws = workbook.add_worksheet(wn)
        ws._fbc_formats = fbc_formats
        for name, func in allfuncs.items():
            setattr(ws, name, functools.partial(func, ws))

    return workbook
