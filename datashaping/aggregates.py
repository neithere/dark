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

# Usage:
#     items = dataset.items(dataset.find(name='John'))
#     a = Avg('age')             # created an aggregation manager: "average age"
#     calc = a.aggregate(items)  # created lazy calculation for given list of dictionaries
#     int(calc)                  # here the calculation is actually done
#     int(calc)                  # cached result returned, no recalc

__doc__ = """
>>> import yaml
>>> from db import Dataset
>>> people = Dataset(yaml.load(open('example_data/people.yaml'))).all()

# All aggregates

>>> int(Count().count_for(people))
14
>>> int(Avg('age').count_for(people))
86
>>> int(Min('age').count_for(people))
40
>>> int(Max('age').count_for(people))
232
>>> int(Sum('age').count_for(people))
1128
>>> int(Qu1('age').count_for(people))
56
>>> int(Qu3('age').count_for(people))
77

# N/A policy

>>> str(Sum('age').count_for(people))
'1128'
>>> str(Sum('age', NA.skip).count_for(people))
'1128'
>>> str(Sum('age', NA.reject).count_for(people))
'None'
>>> str(Min('age').count_for(people))
'40'
>>> str(Min('age', NA.skip).count_for(people))
'40'
>>> str(Min('age', NA.reject).count_for(people))
'None'
"""

__all__ = ['Aggregate', 'Avg', 'Count', 'Max', 'Median', 'Min', 'Sum', 'Qu1', 'Qu3', 'NA']

class AggregationError(Exception):
    pass

class LazyCalculation(object):
    def __init__(self, agg, values):
        self.agg    = agg
        self.values = values
        self.result = None
    def get_result(self):
        if len(self.values) == 0:
            return None
        try:
            self.result = self.result or self.agg.calc(self.values)
        except TypeError, e:
            raise AggregationError, 'Could not perform %s aggregation on key "%s": '\
                                    'data contains a non-numeric value. Original '\
                                    'message: %s' % (self.agg.name(), self.agg.key, e.message)
        return self.result
    __int__     = lambda self: int(self.get_result())
    __float__   = lambda self: float(self.get_result())
    __str__     = lambda self: str(self.get_result())
    __repr__    = lambda self: u'<lazy %s by %s>' % (self.agg.name, '%d values'%len(self.values) if len(self.values) > 3 else self.values)

class NA(object):
    "Policy against N/A values. To be used in Aggregate constructors: Min('key', NA.skip)."
    skip, reject = 1, 2
    __str__  = lambda self: 'N/A'
    __repr__ = lambda self: '<N/A>'

class Aggregate(object):
    def __init__(self):
        self.key = None
    __str__  = lambda self: '%s(%s)' % (self.__class__.__name__, self.key or 'all')
    __repr__ = lambda self: '<%s>' % str(self)
    name = lambda self: self.__class__.__name__

class AggregateManager(Aggregate):
    def __init__(self, key, na_policy=NA.skip):
        self.key = key
        self.na_policy = na_policy
    def count_for(self, dictionaries):
        values = []
        for item in dictionaries:
            value = item.get(self.key, None)
            if value is None:
                # decide what to do if a None is found in values (i.e. a value is not available)
                if self.na_policy is NA.reject:
                    # reset the whole calculated value to None if at least one value is N/A
                    return None
                elif self.na_policy is NA.skip:
                    # silently ignore items with empty values, count only existing integers; same as "rm.na" in R (?)
                    continue
            values.append(value)
        if values:
            return LazyCalculation(self, values)
        else:
            return NA()
    def calc(self, values):
        raise NotImplementedError

# CLASSES THAT INHERIT TO AggregateManager

class Avg(AggregateManager):
    @staticmethod
    def calc(values):
        return float(sum(values, 0)) / len(values)

class Max(AggregateManager):
    @staticmethod
    def calc(values):
        return max(values)

class Median(AggregateManager):
    """
    Given a vector V of length N, the median of V is the middle value of a sorted
    copy of V, V_sorted - i.e., V_sorted[(N-1)/2], when N is odd. When N is even,
    it is the average of the two middle values of V_sorted.
    """
    @staticmethod
    def calc(values):
        values = sorted(values)
        middle = len(values)>>1
        # when length is odd
        if len(values) % 2:
            # the median is the middle value
            return values[middle]
        # when length is even
        else:
            # it is the average of the two middle values
            lower = middle - 1
            upper = middle + 1
            return sum(values[lower:upper]) / 2.0

"""
Almost as commonly used as the median are the quartiles, q0.25 and
q0.75. Usually these are called the lower and upper quartiles,
respectively. They are located halfway between the median, q0.5, and the
extremes, x(1) and x(n). In typically colorful terminology, Tukey (1977)
calls q0.25 and q0.75 the 'hinges', imagining that the data set has been
folded first at the median, and the quartiles.
-- http://mail.python.org/pipermail/python-list/2002-March/134190.html
"""

class Qu1(Median):
    "Calculates the q0.25. (NOTE: stub!)"
    def calc(self, values):
        l = len(values) / 4
        return super(Qu1, self).calc(values[:l])
class Qu3(Median):
    "Calculates the q0.75. (NOTE: stub!)"
    def calc(self, values):
        l = (len(values) / 4) * 3
        return super(Qu3, self).calc(values[l:])

class Min(AggregateManager):
    @staticmethod
    def calc(values):
        return min(values)

class Sum(AggregateManager):
    @staticmethod
    def calc(values):
        return sum(values, 0)

class Count(AggregateManager):
    """
    Counts distinct values for given key. If key is not specified, simply counts
    all items in the query.
    """
    def __init__(self, key=None, na_policy=NA.skip):        # TODO: err_policy (skip, raise, set N/A, set 0)
        self.key = key
        self.na_policy = na_policy
        # overload resource-consuming parent method if we can do without it
        if not key:
            self.count_for = self.calc
    @staticmethod
    def calc(values):
        return len(set(values))

if __name__=='__main__':
    import doctest
    doctest.testmod()
