# -*- coding: utf-8 -*-

import db
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
>>> data = yaml.load(open('people.yaml'))
>>> people = Dataset(data)
>>> q = people.all()

# note: function cast_cons() is a simple wrapper for cast(), prints a basic table

# count, no grouper factors, implicit aggregate

>>> cast_cons(q)
+------------+
| Count(all) |
+------------+
|         14 |
+------------+

# count, implicit aggregate

>>> cast_cons(q, ['gender'])
+--------+------------+
| gender | Count(all) |
+--------+------------+
| female |          2 |
|   male |         12 |
+--------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['gender'], [], Count())
+--------+------------+
| gender | Count(all) |
+--------+------------+
| female |          2 |
|   male |         12 |
+--------+------------+

>>> cast_cons(q, [], ['gender'], Count())
+--------+------+------------+
| female | male | Count(all) |
+--------+------+------------+
|      2 |   12 |         14 |
+--------+------+------------+

# count, implicit aggregate

>>> cast_cons(q, ['country'])
+-------------+------------+
|     country | Count(all) |
+-------------+------------+
|     England |          2 |
|     Finland |          1 |
| Netherlands |          1 |
| New Zealand |          1 |
|      Norway |          1 |
|      Sweden |          1 |
| Switzerland |          1 |
|         USA |          6 |
+-------------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['country'], [], Count())
+-------------+------------+
|     country | Count(all) |
+-------------+------------+
|     England |          2 |
|     Finland |          1 |
| Netherlands |          1 |
| New Zealand |          1 |
|      Norway |          1 |
|      Sweden |          1 |
| Switzerland |          1 |
|         USA |          6 |
+-------------+------------+

# count, multiple factors (two)

>>> cast_cons(q, ['country','city'], [], Count())
+-------------+---------------------+------------+
|     country |                city | Count(all) |
+-------------+---------------------+------------+
|     England |    Great Torrington |          1 |
|     England |  Maida Vale, London |          1 |
|     Finland |            Helsinki |          1 |
| Netherlands |           Rotterdam |          1 |
| New Zealand |            Auckland |          1 |
|      Norway |                Oslo |          1 |
|      Sweden |           Stockholm |          1 |
| Switzerland |          Winterthur |          1 |
|         USA | Betty Jean Jennings |          1 |
|         USA |             Chicago |          1 |
|         USA |           Milwaukee |          1 |
|         USA |            New York |          1 |
|         USA |                None |          1 |
|         USA |            San Jose |          1 |
+-------------+---------------------+------------+

# count, multiple factors (three)

>>> cast_cons(q, ['country','city','gender'], [], Count())
+-------------+---------------------+--------+------------+
|     country |                city | gender | Count(all) |
+-------------+---------------------+--------+------------+
|     England |    Great Torrington |   male |          1 |
|     England |  Maida Vale, London |   male |          1 |
|     Finland |            Helsinki |   male |          1 |
| Netherlands |           Rotterdam |   male |          1 |
| New Zealand |            Auckland |   male |          1 |
|      Norway |                Oslo |   male |          1 |
|      Sweden |           Stockholm |   male |          1 |
| Switzerland |          Winterthur |   male |          1 |
|         USA | Betty Jean Jennings | female |          1 |
|         USA |             Chicago |   male |          1 |
|         USA |           Milwaukee |   male |          1 |
|         USA |            New York |   male |          1 |
|         USA |                None | female |          1 |
|         USA |            San Jose |   male |          1 |
+-------------+---------------------+--------+------------+

# explicit custom aggregate, no grouper factor

>>> cast_cons(q, [], [], Avg('age'))
+---------------+
|      Avg(age) |
+---------------+
| 86.7692307692 |
+---------------+

>>> cast_cons(q, [], [], Median('age'))
+-------------+
| Median(age) |
+-------------+
|          79 |
+-------------+

# aggregate by one key, Avg

>>> cast_cons(q, ['country'], [], Avg('age'))
+-------------+----------+
|     country | Avg(age) |
+-------------+----------+
|     England |    164.5 |
|     Finland |     40.0 |
| Netherlands |     79.0 |
| New Zealand |      N/A |
|      Norway |     83.0 |
|      Sweden |    102.0 |
| Switzerland |     75.0 |
|         USA |     70.0 |
+-------------+----------+

# aggregate by one key, Max

>>> cast_cons(q, ['country'], [], Max('age'))
+-------------+----------+
|     country | Max(age) |
+-------------+----------+
|     England |      232 |
|     Finland |       40 |
| Netherlands |       79 |
| New Zealand |      N/A |
|      Norway |       83 |
|      Sweden |      102 |
| Switzerland |       75 |
|         USA |       88 |
+-------------+----------+

# aggregate by multiple keys (two)

>>> cast_cons(q, ['country','city'], [], Avg('age'))
+-------------+---------------------+----------+
|     country |                city | Avg(age) |
+-------------+---------------------+----------+
|     England |    Great Torrington |    232.0 |
|     England |  Maida Vale, London |     97.0 |
|     Finland |            Helsinki |     40.0 |
| Netherlands |           Rotterdam |     79.0 |
| New Zealand |            Auckland |      N/A |
|      Norway |                Oslo |     83.0 |
|      Sweden |           Stockholm |    102.0 |
| Switzerland |          Winterthur |     75.0 |
|         USA | Betty Jean Jennings |     85.0 |
|         USA |             Chicago |     60.0 |
|         USA |           Milwaukee |     72.0 |
|         USA |            New York |     56.0 |
|         USA |                None |     88.0 |
|         USA |            San Jose |     59.0 |
+-------------+---------------------+----------+

# aggregate by multiple keys (three)

>>> cast_cons(q, ['country','city','gender'], [], Avg('age'))
+-------------+---------------------+--------+----------+
|     country |                city | gender | Avg(age) |
+-------------+---------------------+--------+----------+
|     England |    Great Torrington |   male |    232.0 |
|     England |  Maida Vale, London |   male |     97.0 |
|     Finland |            Helsinki |   male |     40.0 |
| Netherlands |           Rotterdam |   male |     79.0 |
| New Zealand |            Auckland |   male |      N/A |
|      Norway |                Oslo |   male |     83.0 |
|      Sweden |           Stockholm |   male |    102.0 |
| Switzerland |          Winterthur |   male |     75.0 |
|         USA | Betty Jean Jennings | female |     85.0 |
|         USA |             Chicago |   male |     60.0 |
|         USA |           Milwaukee |   male |     72.0 |
|         USA |            New York |   male |     56.0 |
|         USA |                None | female |     88.0 |
|         USA |            San Jose |   male |     59.0 |
+-------------+---------------------+--------+----------+

# multiple aggregates -- NOT YET

#>>> cast_cons(q, ['country','city'], [], [Avg('age'), Min('age'), Max('age')])

# city key not present, level empty, use higher level query

>>> cast_cons(people.find(nick='Kay'), ['name', 'country', 'city'])
+--------------------+---------+------+------------+
|               name | country | city | Count(all) |
+--------------------+---------+------+------------+
| Kathleen Antonelli |     USA | None |          1 |
+--------------------+---------+------+------------+

# city key not present, level empty, use higher level query

>>> cast_cons(q.find(nick='Kay'), ['city'])
+------+------------+
| city | Count(all) |
+------+------------+
| None |          1 |
+------+------------+

# PIVOTING

# pivoting by one factor

>>> cast_cons(q, ['country'], ['gender'])
+-------------+------+--------+------------+
|     country | male | female | Count(all) |
+-------------+------+--------+------------+
|     England |    2 |      0 |          2 |
|     Finland |    1 |      0 |          1 |
| Netherlands |    1 |      0 |          1 |
| New Zealand |    1 |      0 |          1 |
|      Norway |    1 |      0 |          1 |
|      Sweden |    1 |      0 |          1 |
| Switzerland |    1 |      0 |          1 |
|         USA |    4 |      2 |          6 |
+-------------+------+--------+------------+

# pivoting by multiple factors, one level

>>> cast_cons(q.find(country='England'), ['country'], ['city', 'gender'])
+---------+------------------+--------------------+------+------------+
| country | Great Torrington | Maida Vale, London | male | Count(all) |
+---------+------------------+--------------------+------+------------+
| England |                1 |                  1 |    2 |          2 |
+---------+------------------+--------------------+------+------------+

# pivoting by multiple factors, multiple levels

>>> cast_cons(q.find(country=not_('USA')), ['country'], ['city', 'gender'])
+-------------+------------------+--------------------+----------+-----------+----------+------+-----------+------------+------+------------+
|     country | Great Torrington | Maida Vale, London | Helsinki | Rotterdam | Auckland | Oslo | Stockholm | Winterthur | male | Count(all) |
+-------------+------------------+--------------------+----------+-----------+----------+------+-----------+------------+------+------------+
|     England |                1 |                  1 |        0 |         0 |        0 |    0 |         0 |          0 |    2 |          2 |
|     Finland |                0 |                  0 |        1 |         0 |        0 |    0 |         0 |          0 |    1 |          1 |
| Netherlands |                0 |                  0 |        0 |         1 |        0 |    0 |         0 |          0 |    1 |          1 |
| New Zealand |                0 |                  0 |        0 |         0 |        1 |    0 |         0 |          0 |    1 |          1 |
|      Norway |                0 |                  0 |        0 |         0 |        0 |    1 |         0 |          0 |    1 |          1 |
|      Sweden |                0 |                  0 |        0 |         0 |        0 |    0 |         1 |          0 |    1 |          1 |
| Switzerland |                0 |                  0 |        0 |         0 |        0 |    0 |         0 |          1 |    1 |          1 |
+-------------+------------------+--------------------+----------+-----------+----------+------+-----------+------------+------+------------+
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
        Warning: query uniqueness is not checked. If same query provided twice,
        duplicates will occur.
        """
        new_levels = [Level(self,val,query) for val in query.values(self.key)]
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

def cast(basic_query, factor_names=[], pivot_factors=[], aggregate=Count()):
    """
    Returns a table summarizing data grouped by given factors.
    Calculates aggregated values. If aggregate is not defined, Count() is used.
    Pivoting support is not implemented yet.
    """
    # XXX TODO: multiple aggregates (they will also multiply pivot columns)
    
    # XXX this function actually *groups* data and creates a table.
    #     Move the grouping stuff to Query code as a method?

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

        for factor in pivot_factors:
            for level in last_level.query.values(factor):
                query = last_level.query.find(**{factor:level})
                if query.count() and level not in used_pivot_levels[factor]:
                    used_pivot_levels[factor].append(level)

    # append aggregated values
    for row in table:
        last_level = row[-1] # for pivots and "total" aggregates (after pivots are inserted)

        # insert pivot cells
        for factor in pivot_factors:
            for level in used_pivot_levels.get(factor,[]):
                query = last_level.query.find(**{factor:level})
                row.append(aggregate.count_for(query))

        # insert "total" aggregate (by last real, non-pivot column)
        row.append(aggregate.count_for(last_level.query))

    # remove catch-all level
    if not factors:
        for row in table:
            row.pop(0)

    # generate table heading
    table_heading = factor_names
    for factor in pivot_factors:
        table_heading.extend(used_pivot_levels.get(factor,[]))
    table_heading.append(str(aggregate))

    return [table_heading] + table

def cast_cons(*args, **kwargs):
    """
    Wrapper for cast function for usage from console. Prints a simplified table
    using ASCII art. Quick and dirty.
    """
    table = cast(*args, **kwargs)
    maxlens = []
    for row in table:
        for col_i, col in enumerate(row):
            col_len = len(str(col))
            if len(maxlens)-1 < col_i:
                maxlens.append(col_len)
            maxlens[col_i] = max(col_len, maxlens[col_i])
    _hr = lambda i, row: '+-'+ '+'.join('-'*(2+maxlens[idx]) for idx, cell in enumerate(row))[1:] +'+'
    _format_cell = lambda val: '%.2f' % val if isinstance(val, float) else unicode(val)
    for i, row in enumerate(table):
        if i == 0: print _hr(i,row)
        print '| '+ ' | '.join(_format_cell(cell).rjust(maxlens[idx]) for (idx, cell) in enumerate(row)) +' |'
        if i in (0, len(table)-1): print _hr(i,row)

if __name__=='__main__':
    import doctest
    doctest.testmod()
