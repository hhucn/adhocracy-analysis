import collections
import io
import json

import mysql.connector
import mysql.connector.constants
import progress.bar


class keydefaultdict(collections.defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


class NoProgress(object):
    def __init__(self, stream):
        pass

    def update(self):
        pass

    def finish(self):
        pass


class FileProgress(object):
    def __init__(self, stream):
        pos = stream.tell()
        stream.seek(0, 2)
        self.size = stream.tell()
        stream.seek(pos, 0)
        self.bar = progress.bar.Bar('', max=self.size, suffix='%(percent)d%% ETA %(eta)ds')
        self.stream = stream

    def update(self):
        pos = self.stream.tell()
        self.bar.goto(pos)

    def finish(self):
        self.bar.finish()


class DBConnection(object):
    def __init__(self, config):
        self.config = config

    def __enter__(self):
        flags = [mysql.connector.constants.ClientFlag.FOUND_ROWS]
        try:
            host = self.config.get('db_host')
            user = self.config['db_user']
            password = self.config.get('db_password')
            database = self.config['db_database']
        except KeyError as ke:
            raise KeyError('Missing key %s in configuration' % ke.args[0])

        self.db = mysql.connector.connect(
            user=user, host=host, password=password, database=database,
            client_flags=flags)
        self.cursor = self.db.cursor()
        return self

    def execute(self, *args, **kwargs):
        return self.cursor.execute(*args, **kwargs)

    def __iter__(self):
        return iter(self.cursor)

    def commit(self):
        return self.db.commit()

    def __exit__(self, typ, value, traceback):
        self.cursor.close()
        self.db.close()


class Option(object):
    def __init__(self, name, **kwargs):
        self.name = name
        assert 'dest' in kwargs
        self.kwargs = kwargs


def options(option_list):
    def wrapper(func):
        func.option_list = option_list
        return func
    return wrapper


def read_config(args):
    with io.open(args.config_filename, 'r', encoding='utf-8') as configf:
        return json.load(configf)
