# Wrap a sqlite3 database into a python class for simplicity
# 
# Author: Alex Zylstra
# Date: 2014/05/17
# License: MIT

import os
import csv
import sqlite3
import shlex

FILE = 'stocks.db'

class DB:
    """Wrapper class for database operations.

    :param fname: the filename for the sqlite3 database
    :author: Alex Zylstra
    :date: 2014/04/13
    """

    def __init__(self, fname=FILE):
        """Class constructor, which opens a connection to the database"""
        # Create file if it doesn't already exist
        if not os.path.exists(fname):
            file = open(fname, 'w+')
            file.close()

        # wrap in try/catch in case of error
        try:
            # database connector
            self.db = sqlite3.connect(fname)
            # database cursor
            self.c = self.db.cursor()
        except sqlite3.OperationalError:
            print("Error opening database file.")

        # name of the table for the data
        self.TABLE = 'data'
        # keep a separate table of symbols (tickers)
        self.TABLE_SYM = 'tickers'
        # keep a separate table for a detailed text description between column name and what it is
        self.TABLE_INFO = 'info'

        # check to see if the data table exists already
        query = self.c.execute('''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s';''' % self.TABLE)
        # if not, create new table:
        if query.fetchone()[0] == 0:  # table does not exist
            self.c.execute('''CREATE TABLE %s
                (date date, symbol text, open float, low float, high float, target float, high52 float, low52 float, volume float, eps float, eps_est_curr float, eps_est_next float, book float, ebitda float, price_sales float, price_book float, pe float, peg float, peps_est_curr float, peps_est_next float, short float, div_yield float, div float, div_date text, ex_div_date text, day50_avg float, day200_avg float, market_cap float)''' % self.TABLE)

        # check to see if the symbol table exists already
        query = self.c.execute('''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s';''' % self.TABLE_SYM)
        # if not, create new table:
        if query.fetchone()[0] == 0:  # table does not exist
            self.c.execute('''CREATE TABLE %s (symbol text, name text, exchange text, IPO year, sector text, industry text)''' % self.TABLE_SYM)

        # check to see if the info table exists already
        query = self.c.execute('''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s';''' % self.TABLE_INFO)
        # if not, create new table:
        if query.fetchone()[0] == 0:  # table does not exist
            self.c.execute('''CREATE TABLE %s (column text, info text)''' % self.TABLE_INFO)


    def __del__(self):
        """Properly close database objects."""
        self.db.commit()
        self.db.close()

    def num_rows(self, table):
        """Get the number of rows in the given database table associated with this object."""
        assert isinstance(table, str)
        query = self.c.execute('SELECT count(*) from %s' % table)
        return query.fetchone()[0]

    def num_columns(self, table):
        """Get the number of columns in the given table."""
        return len(self.get_columns(table))

    def get_columns(self, table):
        """Get a list of columns in the given table."""
        assert isinstance(table, str)
        query = self.c.execute('PRAGMA table_info(%s)' % table)
        return array_convert(query)

    def get_column_names(self, table):
        """Get a list of column names in the given table."""
        query = self.c.execute('PRAGMA table_info(%s)' % table)
        # get an array of column info:
        columns = array_convert(query)

        # The default SQL result contains other metainfo (data type, etc)
        # Cut it down to just the column names:
        ret = []
        for i in columns:
            ret.append(i[1])
        return ret

    def sql_query(self, query, values=()):
        """Execute a generic SQL query. Be careful!

        :param query: the query to execute, i.e. a string in SQL syntax
        :returns: the result of the query
        """
        return self.c.execute(query, values)
