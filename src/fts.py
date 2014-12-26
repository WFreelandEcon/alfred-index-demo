#!/usr/bin/env python
# encoding: utf-8
from __future__ import print_function, unicode_literals

import csv
from time import time

import search
from workflow import Workflow

WF = Workflow()


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
f = search.FTSFilter(d)
xres = f.filter('kant')
print('Found {0} out of {1} items in {2:0.3}s'.format(len(xres),
                                                      len(d),
                                                      time() - start))
#print(xres[0])
start = time()
f = search.IterFilter(d)
yres = f.filter('kant',
    match_on=search.MATCH_STARTSWITH | search.MATCH_ATOM | search.MATCH_SUBSTRING)
print('Found {0} out of {1} items in {2:0.3}s'.format(len(yres),
                                                      len(d),
                                                      time() - start))
#print(yres[0])

#Results for runs:
# -- First run
#Found 25 out of 44549 items in 0.674s
#Found 27 out of 44549 items in 0.822s
#
#Found 6 out of 44549 items in 0.725s
#Found 3 out of 44549 items in 1.91s
# -- Second run
#Found 25 out of 44549 items in 0.0158s
#Found 27 out of 44549 items in 0.799s
#
#Found 6 out of 44549 items in 0.0201s
#Found 3 out of 44549 items in 1.88s
# -- Third run
#Found 25 out of 44549 items in 0.0198s
#Found 27 out of 44549 items in 0.852s
#
#Found 6 out of 44549 items in 0.0187s
#Found 3 out of 44549 items in 1.84s
