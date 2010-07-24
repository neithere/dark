# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009—2010 Andrey Mikhailenko and contributors
#
#  This file is part of Dark.
#
#  Dark is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

"""
Shaping the data
================

The whole library is built mainly for the function :func:`cast`. It can be used
to build HTML tables, etc. For interactive shell there is a wrapper function
:func:`cast_cons` which redirects arguments to :func:`cast` and prints results
as a nice-looking ASCII table.
"""

import math
from aggregates import *


__all__ = ['cast', 'cast_cons', 'stdev', 'summary']


# TODO: consider syntax like:
#       people.group_by('country','city').pivot_by('gender').annotate(Avg('age'))

# TODO: [0, None, NA] -- they all mean the same. Need precise tests which should be used when.

# TODO: see cast() code for more TODOs :)


class Factor(object):
    """
    A factor is usually represented as a dictionary key or as a column of a RDB
    table.
    """
    def __init__(self, key):
        self.key = key
        self.levels = []

    def __repr__(self):
        return '<Factor {key}>'.format(key=self.key)

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
        new_levels = [Level(self,val,query) for val in
                        sorted(query.values(self.key))] or \
                     [Level(self,None,query)]
        self.levels.extend(new_levels)
        return new_levels


class Level(object):
    "A factor level, i.e. an existing value."
    def __init__(self, factor, value, query):
        self.value = value
        self.query = query.where(**{factor.key: value})
        self.children = []
    def attach(self, levels):
        "Attaches a depending factor level to this level."
        if __debug__:
            for l in levels: assert(isinstance(l, Level))
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

def cast(basic_query, factor_names=None, pivot_factors=None, *aggregates):
    """
    Creates a table summarizing data grouped by given factors. Calculates
    aggregated values. If aggregate is not defined, all items in the query are
    counted. Pivoting (i.e. using factor levels as columns) is also supported.

    The name "cast" stands for "casting melt data" and is a reference to Hadley
    Wickham's package `reshape`_ for R language, though internally these
    packages have little in common.

    .. _reshape: http://had.co.nz/reshape/

    :param basic_query:
        a :class:`Query <dark.query.Query>` instance (pre-filtered or not) on
        which the table is going to be built.

    :param factor_names:
        optional list of keys by which data will be grouped. Their names
        will go into the table heading, and their values will be used to
        calculate aggregated values. If more than one factor is specified, they
        will be grouped hierarchically from left to right.

    :param pivot_factors:
        optional list of keys which values will go into the table heading
        along with factor names so that extra columns with aggregated values
        will be added for each possible factor level (key value).

    :param aggregates:
        optional list of Aggregate instances. Some aggregates require a factor
        name (i.e. key). Examples: `Count()`, `Sum('price')`. Aggregates will
        be calculated for each combination of factors and for each pivoted a
        factor level. If aggregates are not specified,
        :class:`Count <dark.aggregates.Count>` instance is added.

    :returns: a list of lists, i.e. a table.

    See tests for usage examples.
    """

    # XXX this function actually *groups* data and creates a table.
    #     Move the grouping stuff to Query code as a method?

    # note: mutables declared in func signature tend to migrate between calls ;)
    factor_names  = factor_names  or []
    pivot_factors = pivot_factors or []
    aggregates    = aggregates    or [Count()]

    factors = [Factor(n) for n in factor_names]
    for num, factor in enumerate(factors):
        if num == 0:
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
            for level in sorted(last_level.query.values(factor)):
                query = last_level.query.where(**{factor:level})
                if query.count() and level not in used_pivot_levels[factor]:
                    used_pivot_levels[factor].append(level)

    # append aggregated values
    for row in table:
        last_level = row[-1] # for pivots and "total" aggregates (after pivots are inserted)

        # insert pivot cells
        for factor in pivot_factors:
            for level in sorted(used_pivot_levels.get(factor,[])):
                query = last_level.query.where(**{factor:level})
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
    using ASCII art.
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
    (see `summary` function in R language).
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
