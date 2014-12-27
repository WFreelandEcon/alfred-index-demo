#!/usr/bin/env python
# encoding: utf-8
from __future__ import print_function, unicode_literals

import re
import string

import text
from fts import FTSDatabase
from workflow import Workflow

WF = Workflow()

# TODO
# Add fold_diacritics to FTSDatabase
# Add min_score to FTSFilter

# Anchor characters in a name
INITIALS = string.ascii_uppercase + string.digits

# Split on non-letters, numbers
split_on_delimiters = re.compile('[^a-zA-Z0-9]').split

# Match filter flags
MATCH_STARTSWITH = 1
MATCH_CAPITALS = 2
MATCH_ATOM = 4
MATCH_INITIALS_STARTSWITH = 8
MATCH_INITIALS_CONTAIN = 16
MATCH_INITIALS = 24
MATCH_SUBSTRING = 32
MATCH_ALLCHARS = 64
MATCH_ALL = 127


def filter(query, key=lambda x: x, ascending=False,
               include_score=False, min_score=0, max_results=0,
               match_on=MATCH_ALL, fold_diacritics=True):
        pass


class FTSFilter(object):
    def __init__(self, data):
        self.data = data

    def filter(self, query, key=lambda x: x,
               include_score=False, max_results=0, fold_diacritics=True,
               ascending=True, match_on=MATCH_ALL):
        db_file = self._memoize_database(self.data)
        fts = FTSDatabase(self.data, db_file)
        fts.tokenizer = 'porter'

        results = {}
        matched = []
        words = [s.strip() for s in query.split(' ')]
        # Search this virtual table using the various match patters
        # This part of the function will be greatly refactored.
        if match_on & MATCH_SUBSTRING:
            # not fully implemented
            # won't match `x{query}z`
            sql_query = ' '.join([w + '*' for w in words])
            matches = fts.search(sql_query)
            matched += [list(m) + [MATCH_SUBSTRING] for m in matches]
        if match_on & MATCH_STARTSWITH:
            sql_query = '^' + ' '.join(words)
            matches = fts.search(sql_query)
            matched += [list(m) + [MATCH_STARTSWITH] for m in matches]
        if match_on & MATCH_ATOM:
            sql_query = ' '.join(words)
            matches = fts.search(sql_query)
            matched += [list(m) + [MATCH_ATOM] for m in matches]

        queue = set()
        for item in matched:
            score, id_, data, flag = item
            if id_ not in queue:
                results[(score, id_)] = (data, (score * 1000), flag)
                queue.add(id_)

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

    def _memoize_database(self, data):
        # Get hash value of iterable ``data``
        items_hash = str(hash(frozenset(data)))
        # Prepare hidden database file with hash value as name
        hidden_db = '.' + items_hash + '.db'
        # Save the database file so that successive searches on the same
        # dataset have the data cached (in sqlite format), for speed.
        return WF.workflowfile(hidden_db)


class IterFilter(object):
    def __init__(self, data):
        self.data = data
        self._search_pattern_cache = {}

    def filter(self, query, key=lambda x: x, ascending=False,
               include_score=False, min_score=0, max_results=0,
               match_on=MATCH_ALL, fold_diacritics=True):
        """Fuzzy search filter. Returns list of ``items`` that match ``query``.

        ``query`` is case-insensitive. Any item that does not contain the
        entirety of ``query`` is rejected.

        :param query: query to test items against
        :type query: ``unicode``
        :param items: iterable of items to test
        :type items: ``list`` or ``tuple``
        :param key: function to get comparison key from ``items``. Must return
                    a ``unicode`` string. The default simply returns the item.
        :type key: ``callable``
        :param ascending: set to ``True`` to get worst matches first
        :type ascending: ``Boolean``
        :param include_score: Useful for debugging the scoring algorithm.
            If ``True``, results will be a list of tuples
            ``(item, score, rule)``.
        :type include_score: ``Boolean``
        :param min_score: If non-zero, ignore results with a score lower
            than this.
        :type min_score: ``int``
        :param max_results: If non-zero, prune results list to this length.
        :type max_results: ``int``
        :param match_on: Filter option flags. Bitwise-combined list of
            ``MATCH_*`` constants (see below).
        :type match_on: ``int``
        :param fold_diacritics: Convert search keys to ASCII-only
            characters if ``query`` only contains ASCII characters.
        :type fold_diacritics: ``Boolean``
        :returns: list of ``items`` matching ``query`` or list of
            ``(item, score, rule)`` `tuples` if ``include_score`` is ``True``.
            ``rule`` is the ``MATCH_`` rule that matched the item.
        :rtype: ``list``

        **Matching rules**

        By default, :meth:`filter` uses all of the following flags (i.e.
        :const:`MATCH_ALL`). The tests are always run in the given order:

        1. :const:`MATCH_STARTSWITH` : Item search key starts with ``query``
                                       (case-insensitive).
        2. :const:`MATCH_CAPITALS` : The list of capital letters in item search
                                     key starts with ``query``
                                     (``query`` may be lower-case).
                                     E.g.,``of`` would match ``OmniFocus``,
                                     ``gc`` would match ``Google Chrome``
        3. :const:`MATCH_ATOM` : Search key is split into "atoms" on non-word
                                 characters (.,-,' etc.). Matches if ``query``
                                 is one of these atoms (case-insensitive).
        4. :const:`MATCH_INITIALS_STARTSWITH` : Initials are first characters
                                                of the above-described "atoms"
                                                (case-insensitive).
        5. :const:`MATCH_INITIALS_CONTAIN` : ``query`` is a substring of the
                                             above-described initials.
        6. :const:`MATCH_INITIALS` : Combination of (4) and (5).
        7. :const:`MATCH_SUBSTRING` : Match if ``query`` is a substring of
                                      item search key (case-insensitive).
        8. :const:`MATCH_ALLCHARS` : Matches if all characters in ``query``
                                     are in item search key in the same order
                                     (case-insensitive).
        9. :const:`MATCH_ALL` : Combination of all the above.


        ``MATCH_ALLCHARS`` is considerably slower than the other tests and
        provides much less accurate results.

        **Examples:**

        To ignore ``MATCH_ALLCHARS`` (tends to provide the worst matches and
        is expensive to run), use ``match_on=MATCH_ALL ^ MATCH_ALLCHARS``.

        To match only on capitals, use ``match_on=MATCH_CAPITALS``.

        To match only on startswith and substring, use
        ``match_on=MATCH_STARTSWITH | MATCH_SUBSTRING``.

        **Diacritic folding**

        .. versionadded:: 1.3

        If ``fold_diacritics`` is ``True`` (the default), and ``query``
        contains only ASCII characters, non-ASCII characters in search keys
        will be converted to ASCII equivalents (e.g. *ü* -> *u*, *ß* -> *ss*,
        *é* -> *e*).

        See :const:`ASCII_REPLACEMENTS` for all replacements.

        If ``query`` contains non-ASCII characters, search keys will not be
        altered.

        """

        # Remove preceding/trailing spaces
        query = query.strip()

        results = {}

        for i, item in enumerate(self.data):
            skip = False
            score = 0
            words = [s.strip() for s in query.split(' ')]
            value = key(item).strip()
            if value == '':
                continue
            for word in words:
                if word == '':
                    continue
                s, r = self._filter_item(value, word, match_on,
                                         fold_diacritics)

                if not s:  # Skip items that don't match part of the query
                    skip = True
                score += s

            if skip:
                continue

            if score:
                # use "reversed" `score` (i.e. highest becomes lowest) and
                # `value` as sort key. This means items with the same score
                # will be sorted in alphabetical not reverse alphabetical order
                results[(100.0 / score, value.lower(), score)] = (item, score,
                                                                  r)

        # sort on keys, then discard the keys
        keys = sorted(results.keys(), reverse=ascending)
        results = [results.get(k) for k in keys]

        if max_results and len(results) > max_results:
            results = results[:max_results]

        if min_score:
            results = [r for r in results if r[1] > min_score]

        # return list of ``(item, score, rule)``
        if include_score:
            return results
        # just return list of items
        return [t[0] for t in results]

    def _filter_item(self, value, query, match_on, fold_diacritics):
        """Filter ``value`` against ``query`` using rules ``match_on``

        :returns: ``(score, rule)``

        """

        query = query.lower()
        queryset = set(query)

        if not text.isascii(query):
            fold_diacritics = False

        rule = None
        score = 0

        if fold_diacritics:
            value = text.fold_to_ascii(value)

        # pre-filter any items that do not contain all characters
        # of ``query`` to save on running several more expensive tests
        if not queryset <= set(value.lower()):
            return (0, None)

        # item starts with query
        if (match_on & MATCH_STARTSWITH and
                value.lower().startswith(query)):
            score = 100.0 - (len(value) / len(query))
            rule = MATCH_STARTSWITH

        if not score and match_on & MATCH_CAPITALS:
            # query matches capitalised letters in item,
            # e.g. of = OmniFocus
            initials = ''.join([c for c in value if c in INITIALS])
            if initials.lower().startswith(query):
                score = 100.0 - (len(initials) / len(query))
                rule = MATCH_CAPITALS

        if not score:
            if (match_on & MATCH_ATOM or
                    match_on & MATCH_INITIALS_CONTAIN or
                    match_on & MATCH_INITIALS_STARTSWITH):
                # split the item into "atoms", i.e. words separated by
                # spaces or other non-word characters
                atoms = [s.lower() for s in split_on_delimiters(value)]
                # print('atoms : %s  -->  %s' % (value, atoms))
                # initials of the atoms
                initials = ''.join([s[0] for s in atoms if s])

            if match_on & MATCH_ATOM:
                # is `query` one of the atoms in item?
                # similar to substring, but scores more highly, as it's
                # a word within the item
                if query in atoms:
                    score = 100.0 - (len(value) / len(query))
                    rule = MATCH_ATOM

        if not score:
            # `query` matches start (or all) of the initials of the
            # atoms, e.g. ``himym`` matches "How I Met Your Mother"
            # *and* "how i met your mother" (the ``capitals`` rule only
            # matches the former)
            if (match_on & MATCH_INITIALS_STARTSWITH and
                    initials.startswith(query)):
                score = 100.0 - (len(initials) / len(query))
                rule = MATCH_INITIALS_STARTSWITH

            # `query` is a substring of initials, e.g. ``doh`` matches
            # "The Dukes of Hazzard"
            elif (match_on & MATCH_INITIALS_CONTAIN and
                    query in initials):
                score = 95.0 - (len(initials) / len(query))
                rule = MATCH_INITIALS_CONTAIN

        if not score:
            # `query` is a substring of item
            if match_on & MATCH_SUBSTRING and query in value.lower():
                    score = 90.0 - (len(value) / len(query))
                    rule = MATCH_SUBSTRING

        if not score:
            # finally, assign a score based on how close together the
            # characters in `query` are in item.
            if match_on & MATCH_ALLCHARS:
                search = self._search_for_query(query)
                match = search(value)
                if match:
                    score = 100.0 / ((1 + match.start()) *
                                     (match.end() - match.start() + 1))
                    rule = MATCH_ALLCHARS

        if score > 0:
            return (score, rule)
        return (0, None)

    def _search_for_query(self, query):
        if query in self._search_pattern_cache:
            return self._search_pattern_cache[query]

        # Build pattern: include all characters
        pattern = []
        for c in query:
            pattern.append('[^{0}]*{0}'.format(re.escape(c)))
        pattern = ''.join(pattern)
        search = re.compile(pattern, re.IGNORECASE).search

        self._search_pattern_cache[query] = search
        return search
