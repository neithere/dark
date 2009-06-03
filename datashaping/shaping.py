# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Datashaping.
#
#  Datashaping is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

import db
import math
from aggregates import *

# TODO: consider syntax like:
#       people.group_by('country','city').pivot_by('gender').annotate(Avg('age'))

# TODO: [0, None, NA] -- they all mean the same. Need precise tests which should be used when.
#       Also in some cases there are no columns at all -- this is simply unacceptable.

# TODO: see cast() code for more TODOs :)

__doc__ = """
>>> from db import Dataset, in_, not_
>>> from aggregates import *
>>> import yaml
>>> data = yaml.load(open('example_data/people.yaml'))
>>> people = Dataset(data)
>>> q = people.all()

# note: function cast_cons() is a simple wrapper for cast(), prints a basic table

# count, no grouper factors, implicit aggregate

>>> cast_cons(q)
 +------------+
 | Count(all) |
 +------------+
 |         18 |
 +------------+

# count, implicit aggregate

>>> cast_cons(q, ['gender'])
 +--------+------------+
 | gender | Count(all) |
 +--------+------------+
 | female |          3 |
 |   male |         13 |
 +--------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['gender'], [], Count())
 +--------+------------+
 | gender | Count(all) |
 +--------+------------+
 | female |          3 |
 |   male |         13 |
 +--------+------------+

>>> cast_cons(q, ['gender'], [], Count('nick'))
 +--------+-------------+
 | gender | Count(nick) |
 +--------+-------------+
 | female |           1 |
 |   male |           3 |
 +--------+-------------+

>>> cast_cons(q, [], ['gender'], Count('nick'))
 +--------+------+-------------+
 | female | male | Count(nick) |
 +--------+------+-------------+
 |      1 |    3 |           4 |
 +--------+------+-------------+

>>> cast_cons(q, [], ['gender'], Count())
 +--------+------+------------+
 | female | male | Count(all) |
 +--------+------+------------+
 |      3 |   13 |         18 |
 +--------+------+------------+

>>> cast_cons(people.all(), [], ['born__country'], Count('nick'))
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+
 | England | Finland | Netherlands | New Zealand | Norway | South Africa | Sweden | Switzerland | USA | Count(nick) |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+
 |     N/A |     N/A |         N/A |         N/A |    N/A |          N/A |      1 |         N/A |   3 |           4 |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+

# count, implicit aggregate

>>> cast_cons(q, ['born__country'])
 +---------------+------------+
 | born__country | Count(all) |
 +---------------+------------+
 |       England |          2 |
 |       Finland |          1 |
 |   Netherlands |          2 |
 |   New Zealand |          2 |
 |        Norway |          1 |
 |  South Africa |          1 |
 |        Sweden |          1 |
 |   Switzerland |          1 |
 |           USA |          7 |
 +---------------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['born__country'], [], Count())
 +---------------+------------+
 | born__country | Count(all) |
 +---------------+------------+
 |       England |          2 |
 |       Finland |          1 |
 |   Netherlands |          2 |
 |   New Zealand |          2 |
 |        Norway |          1 |
 |  South Africa |          1 |
 |        Sweden |          1 |
 |   Switzerland |          1 |
 |           USA |          7 |
 +---------------+------------+

# count, multiple factors (two)

>>> cast_cons(q, ['born__country','born__city'], [], Count())
 +---------------+---------------------+------------+
 | born__country |          born__city | Count(all) |
 +---------------+---------------------+------------+
 |       England |    Great Torrington |          1 |
 |       England |  Maida Vale, London |          1 |
 |       Finland |            Helsinki |          1 |
 |   Netherlands |           Amsterdam |          1 |
 |   Netherlands |           Rotterdam |          1 |
 |   New Zealand |            Auckland |          2 |
 |        Norway |                Oslo |          1 |
 |  South Africa |            Pretoria |          1 |
 |        Sweden |           Stockholm |          1 |
 |   Switzerland |          Winterthur |          1 |
 |           USA |                None |          1 |
 |           USA | Betty Jean Jennings |          1 |
 |           USA |             Chicago |          1 |
 |           USA |           Milwaukee |          1 |
 |           USA |            New York |          1 |
 |           USA |            San Jose |          1 |
 |           USA |             Seattle |          1 |
 +---------------+---------------------+------------+

# explicit custom aggregate, no grouper factor

>>> cast_cons(q, [], [], Avg('age'))
 +----------+
 | Avg(age) |
 +----------+
 |     79.5 |
 +----------+

>>> cast_cons(q, [], [], Median('age'))
 +-------------+
 | Median(age) |
 +-------------+
 |        73.5 |
 +-------------+

# aggregate by one key, Avg

>>> cast_cons(q, ['born__country'], [], Avg('age'))
 +---------------+----------+
 | born__country | Avg(age) |
 +---------------+----------+
 |       England |    164.5 |
 |       Finland |     40.0 |
 |   Netherlands |     64.0 |
 |   New Zealand |      N/A |
 |        Norway |     83.0 |
 |  South Africa |     41.0 |
 |        Sweden |    102.0 |
 |   Switzerland |     75.0 |
 |           USA |     67.7 |
 +---------------+----------+

# aggregate by one key, Max

>>> cast_cons(q, ['born__country'], [], Max('age'))
 +---------------+----------+
 | born__country | Max(age) |
 +---------------+----------+
 |       England |      232 |
 |       Finland |       40 |
 |   Netherlands |       79 |
 |   New Zealand |      N/A |
 |        Norway |       83 |
 |  South Africa |       41 |
 |        Sweden |      102 |
 |   Switzerland |       75 |
 |           USA |       88 |
 +---------------+----------+

# aggregate by multiple keys

>>> cast_cons(q, ['born__country','born__city','gender'], [], Avg('age'))
 +---------------+---------------------+--------+----------+
 | born__country |          born__city | gender | Avg(age) |
 +---------------+---------------------+--------+----------+
 |       England |    Great Torrington |   male |    232.0 |
 |       England |  Maida Vale, London |   male |     97.0 |
 |       Finland |            Helsinki |   male |     40.0 |
 |   Netherlands |           Amsterdam |   male |     49.0 |
 |   Netherlands |           Rotterdam |   male |     79.0 |
 |   New Zealand |            Auckland |   male |      N/A |
 |        Norway |                Oslo |   male |     83.0 |
 |  South Africa |            Pretoria |   None |     41.0 |
 |        Sweden |           Stockholm |   male |    102.0 |
 |   Switzerland |          Winterthur |   male |     75.0 |
 |           USA |                None | female |     88.0 |
 |           USA | Betty Jean Jennings | female |     85.0 |
 |           USA |             Chicago | female |     60.0 |
 |           USA |           Milwaukee |   male |     72.0 |
 |           USA |            New York |   male |     56.0 |
 |           USA |            San Jose |   male |     59.0 |
 |           USA |             Seattle |   male |     54.0 |
 +---------------+---------------------+--------+----------+

# multiple aggregates -- NOT YET

#>>> cast_cons(q, ['born__country','born__city'], [], [Avg('age'), Min('age'), Max('age')])

# city key not present, level empty, use higher level query

>>> cast_cons(people.find(nick='Kay'), ['name', 'born__country', 'born__city'])
 +--------------------+---------------+------------+------------+
 |               name | born__country | born__city | Count(all) |
 +--------------------+---------------+------------+------------+
 | Kathleen Antonelli |           USA |       None |          1 |
 +--------------------+---------------+------------+------------+

# city key not present, level empty, use higher level query

>>> cast_cons(q.find(nick='Kay'), ['born__city'])
 +------------+------------+
 | born__city | Count(all) |
 +------------+------------+
 |       None |          1 |
 +------------+------------+

# PIVOTING

# pivoting by one factor

>>> cast_cons(q, ['born__country'], ['gender'])
 +---------------+--------+------+------------+
 | born__country | female | male | Count(all) |
 +---------------+--------+------+------------+
 |       England |      0 |    2 |          2 |
 |       Finland |      0 |    1 |          1 |
 |   Netherlands |      0 |    2 |          2 |
 |   New Zealand |      0 |    1 |          2 |
 |        Norway |      0 |    1 |          1 |
 |  South Africa |      0 |    0 |          1 |
 |        Sweden |      0 |    1 |          1 |
 |   Switzerland |      0 |    1 |          1 |
 |           USA |      3 |    4 |          7 |
 +---------------+--------+------+------------+

# pivoting by multiple factors, one level

>>> cast_cons(q.find(born__country='England'), ['born__country'], ['born__city', 'gender'])
 +---------------+------------------+--------------------+------+------------+
 | born__country | Great Torrington | Maida Vale, London | male | Count(all) |
 +---------------+------------------+--------------------+------+------------+
 |       England |                1 |                  1 |    2 |          2 |
 +---------------+------------------+--------------------+------+------------+

# pivoting by multiple factors, multiple levels

>>> cast_cons(q.find(born__country=in_(['Netherlands','Finland'])), ['born__country'], ['born__city', 'gender'])
 +---------------+-----------+----------+-----------+------+------------+
 | born__country | Amsterdam | Helsinki | Rotterdam | male | Count(all) |
 +---------------+-----------+----------+-----------+------+------------+
 |       Finland |         0 |        1 |         0 |    1 |          1 |
 |   Netherlands |         1 |        0 |         1 |    2 |          2 |
 +---------------+-----------+----------+-----------+------+------------+

# pivoting without grouper factors

>>> cast_cons(q, [], ['born__country'], Count())
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+
 | England | Finland | Netherlands | New Zealand | Norway | South Africa | Sweden | Switzerland | USA | Count(all) |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+
 |       2 |       1 |           2 |           2 |      1 |            1 |      1 |           1 |   7 |         18 |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+

# values are unwrapped (in this case some people had more than one occupation)

>>> cast_cons(q, ['occupation'], ['gender'])
 +--------------------------------+--------+------+------------+
 |                     occupation | female | male | Count(all) |
 +--------------------------------+--------+------+------------+
 |                 NetBSD founder |      0 |    0 |          1 |
 |                OpenBSD founder |      0 |    0 |          1 |
 |                OpenSSH founder |      0 |    0 |          1 |
 |           President of the FSF |      0 |    1 |          1 |
 |              Software engineer |      0 |    1 |          1 |
 |        author of Perl language |      0 |    1 |          1 |
 |      author of Python language |      0 |    1 |          1 |
 |           author of R language |      0 |    1 |          2 |
 |              computer engineer |      0 |    1 |          1 |
 |      computer language pioneer |      0 |    1 |          1 |
 |            computer programmer |      0 |    1 |          1 |
 |             computer scientist |      0 |    4 |          4 |
 |                   cryptanalyst |      0 |    1 |          1 |
 | invented mechanical calculator |      0 |    1 |          1 |
 |                       logician |      0 |    1 |          1 |
 |                  mathematician |      0 |    2 |          2 |
 |      original ENIAC programmer |      1 |    0 |          1 |
 |                     polititian |      0 |    1 |          1 |
 |                     programmer |      1 |    1 |          2 |
 +--------------------------------+--------+------+------------+

# all row cells are present regardless of data availability in grouper columns

>>> cast_cons(q, ['born__country', 'nick'], [], Count('nick'))
 +---------------+-------+-------------+
 | born__country |  nick | Count(nick) |
 +---------------+-------+-------------+
 |       England |  None |         N/A |
 |       Finland |  None |         N/A |
 |   Netherlands |  None |         N/A |
 |   New Zealand |  None |         N/A |
 |        Norway |  None |         N/A |
 |  South Africa |  None |         N/A |
 |        Sweden | Conny |           1 |
 |   Switzerland |  None |         N/A |
 |           USA |   Kay |           1 |
 |           USA |   Woz |           1 |
 |           USA |   rms |           1 |
 +---------------+-------+-------------+


# summary function

>>> summary(q, 'age')
 +-----+---------+--------+---------+---------+-----+
 | min | 1st qu. | median | average | 3rd qu. | max |
 +-----+---------+--------+---------+---------+-----+
 |  40 |    99.5 |   73.5 |    79.5 |    45.0 | 232 |
 +-----+---------+--------+---------+---------+-----+

# standard deviation function

>>> stdev(q, 'age')
42.176101401288442

# nested structures

>>> cast_cons(q, ['fullname__first'])
 +-----------------+------------+
 | fullname__first | Count(all) |
 +-----------------+------------+
 |            Alan |          1 |
 |           Anita |          1 |
 |          Donald |          1 |
 |           Guido |          1 |
 |           Linus |          1 |
 |         Richard |          1 |
 |         Stephen |          1 |
 |            Theo |          1 |
 +-----------------+------------+

>>> cast_cons(q, ['gender'], ['fullname__first'])
 +--------+------+-------+--------+-------+-------+---------+---------+------------+
 | gender | Alan | Anita | Donald | Guido | Linus | Richard | Stephen | Count(all) |
 +--------+------+-------+--------+-------+-------+---------+---------+------------+
 | female |    0 |     1 |      0 |     0 |     0 |       0 |       0 |          3 |
 |   male |    1 |     0 |      1 |     1 |     1 |       1 |       1 |         13 |
 +--------+------+-------+--------+-------+-------+---------+---------+------------+

"""

class Factor(object):
    "A factor is usually represented as a dictionary key or as a column of a RDB table."
    def __init__(self, key):
        self.key = key
        self.levels = []
    __repr__ = lambda self: '<Factor %s>' % self.key
    def __iter__(self):
        return iter(self.levels)
    def add_levels(self, query):
        """
        Finds factor levels filtered by given query and appends them to the whole
        list of levels. Returns only the newly found levels.
        
        If no level could be found, a dummy empty level is inserted so that
        all columns are present regardless of data availability.
        
        Warning: query uniqueness is not checked. If same query provided twice,
        duplicates will occur.
        """
        new_levels = [Level(self,val,query) for val in query.values_for(self.key)] or [Level(self,None,query)]
        self.levels.extend(new_levels)
        return new_levels

class Level(object):
    "A factor level, i.e. an existing value."
    def __init__(self, factor, value, query):
        self.value = value
        self.query = query.find(**{factor.key: value})
        self.children = []
    def attach(self, levels):
        "Attaches a depending factor level to this level."
        for l in levels: assert(isinstance(l, Level))          # DEBUG
        self.children.extend(levels)
    def get_rows(self):
        "Returns table rows. If more than one child exists, duplicate self for each of them."
        if self.children:
            for child in self.children:
                for row in child.get_rows():
                    yield [self] + row
        else:
            yield [self]
    __repr__ = lambda self: '<Level %s>' % self.value
    __unicode__ = lambda self: unicode(self.value)
    __str__ = lambda self: str(self.value)

class CatchAllLevel(object):
    """"
    Dummy level representing 'SELECT * FROM ...' query. Inserted into the table
    if grouper factors are not specified.
    """
    def __init__(self, query):
        self.query = query
    __unicode__ = __str__ = lambda self: '(all)'

"""
    group_by -- по каким ключам сгруппировать данные (ключ идет в заголовок столбца, значение -- в ячейку)
    pivot_by -- по каким значениям дополнительно сгруппировать данные (это уже НЕ pipeline; значение идет в заголовок столбца)
    agg(key) -- по какому ключу получать значение и какой функцией его обрабатывать (результат идет в ячейку pivot)

    если сводные значения не указаны, делаем просто общий агрегат.

    смысл "отливки" (cast) в формировании _табличной_ формы; перед этим делается группировка -- можно её код убрать в Query.
"""

def cast(basic_query, factor_names=None, pivot_factors=None, *aggregates):
    """
    Returns a table summarizing data grouped by given factors.
    Calculates aggregated values. If aggregate is not defined, Count() is used.
    Supports pivoting (i.e. using factor levels as columns).
    """
    # XXX TODO: multiple aggregates (they will also multiply pivot columns)
    
    # XXX this function actually *groups* data and creates a table.
    #     Move the grouping stuff to Query code as a method?

    # note: do not declare the lists in func sig or they will migrate between calls :)
    factor_names  = factor_names  or []
    pivot_factors = pivot_factors or []
    aggregates    = aggregates    or [Count()]

    factors = [Factor(n) for n in factor_names]
    for num, factor in enumerate(factors):
        if num is 0:
            # find all available levels
            factor.add_levels(basic_query)
        else:
            # find levels for each super level (i.e. each level of parent factor)
            for super_level in factors[num-1]:
                # find levels filtered by that super level
                levels = factor.add_levels(super_level.query)
                # inform the super level about these nested levels (so that it can poll them later)
                super_level.attach(levels)

    # build the table    (can be extracted to another function)

    table = []
    # poll levels of the first factor; they will recursively gather information
    # from attached levels of other factors. This may result in multiple rows per level.
    if factors:
        for level in factors[0]:
            level_rows = level.get_rows()
            for row in level_rows:
                row_last_level = row[-1]
                table.append(row)
    else:
        # a dummy level representing "SELECT * FROM ..." query
        table.append([CatchAllLevel(basic_query)])

    # XXX we do _not_ use hierarchy _within_ pivots. Is this correct?

    # XXX this code is almost completely duplicated below, where we insert
    #     pivot cells and total aggregates into rows. is there a way to unify?

    # collect pivot levels filtered by all grouper factors
    used_pivot_levels = dict((k,[]) for k in pivot_factors)
    for row in table:
        last_level = row[-1]

        # collect pivot levels
        for factor in pivot_factors:
            for level in last_level.query.values_for(factor):
                query = last_level.query.find(**{factor:level})
                if query.count() and level not in used_pivot_levels[factor]:
                    used_pivot_levels[factor].append(level)

    # append aggregated values
    for row in table:
        last_level = row[-1] # for pivots and "total" aggregates (after pivots are inserted)

        # insert pivot cells
        for factor in pivot_factors:
            for level in sorted(used_pivot_levels.get(factor,[])):
                query = last_level.query.find(**{factor:level})
                for aggregate in aggregates:
                    row.append(aggregate.count_for(query))

        # insert "total" aggregates (by last real, non-pivot column)
        for aggregate in aggregates:
            row.append(aggregate.count_for(last_level.query))

    # remove catch-all level
    if not factors:
        for row in table:
            row.pop(0)

    # generate table heading
    table_heading = factor_names
    for factor in pivot_factors:
        for level in sorted(used_pivot_levels.get(factor,[])):
            if len(aggregates) < 2:
                table_heading.append(level)
            else:
                for aggregate in aggregates:
                    table_heading.append('%s %s' % (level, aggregate))
    for aggregate in aggregates:
        table_heading.append(str(aggregate))

    return [table_heading] + table

def cast_cons(*args, **kwargs):
    """
    Wrapper for cast function for usage from console. Prints a simplified table
    using ASCII art. Quick and dirty.
    """
    print_table(cast(*args, **kwargs))

def print_table(table):
    "Prints a list of lists as a nice-looking ASCII table."
    def _format_cell(val):
        # handle lazy calculation (it can be coerced to str/unicode/int/float but we want nicer results)
        if hasattr(val, 'get_result'):
            n = val.get_result()
            if isinstance(n, float):
                return '%.1f' % n
        return unicode(val)
    maxlens = []
    for row in table:
        for col_i, col in enumerate(row):
            col_len = len(_format_cell(col))
            if len(maxlens)-1 < col_i:
                maxlens.append(col_len)
            maxlens[col_i] = max(col_len, maxlens[col_i])
    _hr = lambda i, row: ' +-'+ '+'.join('-'*(2+maxlens[idx]) for idx, cell in enumerate(row))[1:] +'+'
    for i, row in enumerate(table):
        if i == 0: print _hr(i,row)
        print ' | '+ ' | '.join(_format_cell(cell).rjust(maxlens[idx]) for (idx, cell) in enumerate(row)) +' |'
        if i in (0, len(table)-1): print _hr(i,row)

def print_table_rotated():
    "Same as print_table but rotated 90 by degrees clockwise."
    # XXX this should be an option for cast(), not cast_cons()
    raise NotImplementedError, 'sorry, table rotation is not supported yet.'

def summary(query, key):
    """
    Prints a summary for given key in given query.
    (see 'summary' function in R language).
    """
    head = ('min', '1st qu.', 'median', 'average', '3rd qu.', 'max')
    stats = (
        Min(key).count_for(query),
        Qu1(key).count_for(query),
        Median(key).count_for(query),
        Avg(key).count_for(query),
        Qu3(key).count_for(query),
        Max(key).count_for(query),
    )
    print_table([head, stats])

def stdev(query, key):
    "Prints standard deviation for given key in given query."
    avg = Avg(key).count_for(query)
    deviations = [d[key] - int(avg) for d in query if key in d]
    variance = sum(x*x for x in deviations) / float(len(query)-1)
    return math.sqrt(variance)

if __name__=='__main__':
    import doctest
    doctest.testmod()
