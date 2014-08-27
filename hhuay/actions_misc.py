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
    # TODO # users
    # TODO # raw requests
    # TODO # non-stats requests
    # TODO # votes
    # TODO # comments
