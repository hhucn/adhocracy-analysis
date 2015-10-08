
from .util import parse_date


def filter_config_dates(values, config):
    start_ts = parse_date(config['startdate'])
    end_ts = parse_date(config['enddate'])
    return filter_date(values, start_ts, end_ts)


def filter_date(values, start, end):
    assert isinstance(start, int)
    assert isinstance(end, int)
    for v in values:
        if v.time >= start and v.time <= end:
            yield v
