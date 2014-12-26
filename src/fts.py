#!/usr/bin/env python
# encoding: utf-8
from __future__ import print_function, unicode_literals

import sqlite3
import struct
import os.path


class FTSDatabase(object):
    def __init__(self, path=None):
        self._path = path or ':memory:'
        self._table = 'filter'
        self._fields = 'id, data'
        self.con = sqlite3.connect(self._path)

    # Properties  -------------------------------------------------------------

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, value):
        self._fields = value

    # API  --------------------------------------------------------------------

    def create(self, data, table=None, fields=None):
        # Allow for dynamic table and field names
        self.table = table or self._table
        self.fields = fields or self._fields

        with self.con:
            cur = self.con.cursor()
            # Create virtual table if new database
            if not os.path.exists(self.path):
                print('creating...')
                sql = ('CREATE VIRTUAL TABLE {table} '
                       'USING fts3({columns})')
                sql = sql.format(table=self.table,
                                 columns=self.fields)
                self._execute(cur, sql)
                # Fill and index virtual table
                sql = None
                for i, item in enumerate(data):
                    values = self._prepare_values(i, item)
                    if not sql:
                        sql = ('INSERT OR IGNORE INTO {table} '
                               '({columns}) VALUES ({data})')
                        sql = sql.format(table=self.table,
                                         columns=self.fields,
                                         data=', '.join('?' * len(values)))
                    cur.execute(sql, values)

    def search(self, query, ranks=None):
        # nested SELECT to keep from calling the rank function
        # multiple times per row.
        sql = ('SELECT * FROM '
               '(SELECT rank(matchinfo({table})) '
               'AS score, {columns} '
               'FROM {table} '
               'WHERE {table} MATCH "{query}") '
               'ORDER BY score DESC;').format(table=self.table,
                                              columns=self.fields,
                                              query=query)
        # `Row` provides both index-based and case-insensitive name-based access
        # to columns with almost no memory overhead
        self.con.row_factory = sqlite3.Row
        with self.con:
            cur = self.con.cursor()
            ranks = ranks or [1.0] * len(self.fields)
            self.con.create_function('rank', 1, self.make_rank_func(ranks))
            cur.execute(sql)
            return cur.fetchall()

    ## Helper Methods  --------------------------------------------------------

    def _execute(self, cur, sql):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError as err:
            exists_error = b'table {} already exists'.format(self.table)
            if err.message == exists_error:
                pass
            elif b'malformed MATCH' in err.message:
                return 'Invalid query'
            else:
                raise err

    def _prepare_values(self, i, item):
        values = [i, item]
        if hasattr(item, '__iter__'):
            values = [self._quote(self._unquote(x))
                      for x in item]
        return values

    @staticmethod
    def _quote(text):
        return '"' + text + '"'

    @staticmethod
    def _unquote(text):
        return text.replace('"', "'")

    @staticmethod
    def make_rank_func(weights):
        """Search ranking function.

        Use floats (1.0 not 1) for more accurate results. Use 0 to ignore a
        column.

        Adapted from <http://goo.gl/4QXj25> and <http://goo.gl/fWg25i>

        :param weights: list or tuple of the relative ranking per column.
        :type weights: :class:`tuple` OR :class:`list`
        :returns: a function to rank SQLITE FTS results
        :rtype: :class:`function`

        """
        def rank(matchinfo):
            """
            `matchinfo` is defined as returning 32-bit unsigned integers in
            machine byte order (see http://www.sqlite.org/fts3.html#matchinfo)
            and `struct` defaults to machine byte order.
            """
            bufsize = len(matchinfo)  # Length in bytes.
            matchinfo = [struct.unpack(b'I', matchinfo[i:i + 4])[0]
                         for i in range(0, bufsize, 4)]
            it = iter(matchinfo[2:])
            return sum(x[0] * w / x[1]
                       for x, w in zip(zip(it, it, it), weights)
                       if x[1])
        return rank
