# -*- coding: utf-8 -*-

# Usage:
#     items = dataset.items(dataset.find(name='John'))
#     a = Avg('age')             # created an aggregation manager: "average age"
#     calc = a.aggregate(items)  # created lazy calculation for given list of dictionaries
#     int(calc)                  # here the calculation is actually done
#     int(calc)                  # cached result returned, no recalc

__doc__ = """
>>> import yaml
>>> items = yaml.load(open('people.yaml'))

# All aggregates

>>> int(Count().count_for(items))
7
>>> int(Avg('age').count_for(items))
24
>>> int(Min('age').count_for(items))
2
>>> int(Max('age').count_for(items))
35
>>> int(Sum('age').count_for(items))
148
>>> int(Median('age').count_for(items))
28

# N/A policy

>>> str(Sum('age').count_for(items))
'148'
>>> str(Sum('age', NA.skip).count_for(items))
'148'
>>> str(Sum('age', NA.reject).count_for(items))
'None'
>>> str(Min('age').count_for(items))
'2'
>>> str(Min('age', NA.skip).count_for(items))
'2'
>>> str(Min('age', NA.reject).count_for(items))
'None'
"""

__all__ = ['Aggregate', 'Avg', 'Max', 'Median', 'Min', 'Sum', 'Count', 'NA']

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
                                    'message: %s' % (self.agg.name, self.agg.key, e.message)
        return self.result
    __int__     = lambda self: int(self.get_result())
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
    def __init__(self, key=None, na_policy=NA.skip):
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
