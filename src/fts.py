#!/usr/bin/env python
# encoding: utf-8
from __future__ import print_function, unicode_literals

import csv
import os.path
import sqlite3
import struct
from time import time
import workflow as wf
from workflow import Workflow

WF = Workflow()

# Match filter flags
MATCH_STARTSWITH = 1
MATCH_ATOM = 2
MATCH_SUBSTRING = 4
MATCH_ALL = 7


def filter(query, items, key=lambda x: x,
           include_score=False, max_results=0, fold_diacritics=True,
           ascending=True, match_on=MATCH_ALL):
    # Get hash value of iterable ``items``
    items_hash = str(hash(frozenset(items)))
    # Prepare database file with hash value as name
    hidden_db = '.' + items_hash + '.db'
    # Save the database file so that successive searches on the same
    # dataset have the data cached (in sqlite format), for speed.
    database = WF.workflowfile(hidden_db)
    # If this item set not searched before, create FTS database
    if not os.path.exists(database):
        con = sqlite3.connect(database)
        with con:
            cur = con.cursor()
            # Create the FTS virtual table with only two columns:
            # id and data, where `data` is the search string key
            # and `id` is simply a unique id
            # At some later date, I'd like to add the ability to customize
            # the columns if you have structured data (like a dict)...
            cur.execute("""CREATE VIRTUAL TABLE filter
                           USING fts3(id, data)""")
            # Add the data to the virtual table
            for i, item in enumerate(items):
                value = key(item).strip()
                if value == '':
                    continue
                cur.execute("""INSERT OR IGNORE INTO
                            filter (id, data)
                            VALUES (?, ?)
                            """, (i, value))
    # Ensure connection
    con = sqlite3.connect(database)
    # Row provides both index-based and case-insensitive name-based access
    # to columns with almost no memory overhead
    con.row_factory = sqlite3.Row

    results = {}
    with con:
        # Add ranking function to database connection
        con.create_function('rank', 1, make_rank_func((0, 1.0)))
        cur = con.cursor()
        words = [s.strip() for s in query.split(' ')]
        # nested SELECT to keep from calling the rank function
        # multiple times per row.
        sql_query = """SELECT id, score, data FROM
                       (SELECT rank(matchinfo(filter))
                        AS score, id, data
                        FROM filter
                        WHERE filter MATCH "{}")
                        ORDER BY score DESC;"""
        # Search this virtual table using the various match patters
        # This part of the function will be greatly refactored.
        if match_on & MATCH_SUBSTRING:
            sql_query = sql_query.format(' '.join([w + '*' for w in words]))
            cur.execute(sql_query)
            res = cur.fetchall()
            for i in res:
                results[(i[1], i[0])] = (i[2], (i[1] * 100), MATCH_SUBSTRING)
        if match_on & MATCH_STARTSWITH:
            sql_query = sql_query.format('^' + ' '.join(words))
            cur.execute(sql_query)
            res = cur.fetchall()
            for i in res:
                results[(i[1], i[0])] = (i[2], (i[1] * 100), MATCH_STARTSWITH)
        if match_on & MATCH_ATOM:
            sql_query = sql_query.format(' '.join(words))
            cur.execute(sql_query)
            res = cur.fetchall()
            for i in res:
                results[(i[1], i[0])] = (i[2], (i[1] * 100), MATCH_ATOM)

    # sort on keys, then discard the keys
    keys = sorted(results.keys(), reverse=ascending)
    results = [results.get(k) for k in keys]

    if max_results and len(results) > max_results:
        results = results[:max_results]

    # return list of ``(score, item, rule)``
    if include_score:
        return results
    # just return list of items
    return [t[0] for t in results]


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


def books_data():
    d = []
    with open(WF.workflowfile('books.tsv'), 'rb') as file:
        reader = csv.reader(file, delimiter=b'\t')
        for row in reader:
            id_, author, title, url = [v.decode('utf-8') for v in row]
            s = ' '.join([author, title])
            d.append(s)
    return d

d = books_data()
start = time()
res = filter('kant', d, include_score=True)
print('Found {0} out of {1} items in {2:0.3}s'.format(len(res),
                                                      len(d),
                                                      time() - start))
#print(res)
start = time()
res = WF.filter('kant', d, include_score=True,
                match_on=wf.MATCH_STARTSWITH | wf.MATCH_ATOM | wf.MATCH_SUBSTRING)
print('Found {0} out of {1} items in {2:0.3}s'.format(len(res),
                                                      len(d),
                                                      time() - start))
#print(res)

#Results for runs:
# -- First run
#Found 25 out of 44549 items in 0.674s
#Found 27 out of 44549 items in 0.822s
# -- Second run
#Found 25 out of 44549 items in 0.0158s
#Found 27 out of 44549 items in 0.799s
# -- Third run
#Found 25 out of 44549 items in 0.0198s
#Found 27 out of 44549 items in 0.852s
