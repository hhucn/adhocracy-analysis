import mysql.connector
import mysql.connector.constants
import re


class DBConnection(object):

    def __init__(self, config, autocommit=True):
        self.config = config
        self._autocommit = autocommit
        self._committed = False
        self._progress_bars = []

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
            client_flags=flags, get_warnings=True, raise_on_warnings=True)
        self.cursor = self.db.cursor()
        return self

    def execute(self, sql, *args, **kwargs):
        return self.cursor.execute(sql, *args, **kwargs)

    def executemany(self, sql, *args, **kwargs):
        return self.cursor.executemany(sql, *args, **kwargs)

    def affected_rows(self):
        return self.cursor._rowcount

    def __iter__(self):
        return iter(self.cursor)

    def commit(self):
        self._committed = True
        return self.db.commit()

    def __exit__(self, typ, value, traceback):
        if self._autocommit and not self._committed:
            self.commit()
        for pb in self._progress_bars:
            pb.__exit__(typ, value, traceback)
        self.cursor.close()
        self.db.close()

    def register_bar(self, pb):
        self._progress_bars.append(pb)

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    def simple_query(self, sql, *args):
        """ 1 column query, return as a plain list """
        self.execute(sql, *args)
        return [r[0] for r in self]

    def drop_table(self, tblname):
        # No prepared statements, so lets be sure the table name is kosher
        assert re.match(r'^[a-zA-Z_0-9]+$', tblname)
        try:
            self.execute('DROP TABLE IF EXISTS %s;' % tblname)
        except mysql.connector.errors.DatabaseError as de:
            if de.errno != 1051:  # Warning for table not found
                raise

    def recreate_table(self, tblname, columns_sql):
        self.drop_table(tblname)
        assert re.match(r'^[a-zA-Z_0-9]+$', tblname)
        self.execute('CREATE TABLE %s (%s)' % (tblname, columns_sql))
