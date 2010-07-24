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

# Usage:
#     items = dataset.items(dataset.find(name='John'))
#     a = Avg('age')             # created an aggregation manager: "average age"
#     calc = a.aggregate(items)  # created lazy calculation for given list of dictionaries
#     int(calc)                  # here the calculation is actually done
#     int(calc)                  # cached result returned, no recalc

"""
Aggregates
==========
"""

from decimal import Decimal


__all__ = ['Aggregate', 'Avg', 'Count', 'Max', 'Median', 'Min', 'Sum', 'Qu1', 'Qu3', 'NA']


DECIMAL_EXPONENT = Decimal('.01')    # XXX let user change this


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
            raise AggregationError('Could not perform %s aggregation on key '
                                   '"%s" data contains a non-numeric value. '
                                   'Original message: %s' % (self.agg.name(),
                                   self.agg.key, e.message))
        #if isinstance(self.result, Decimal):
        #    # we don't want tens of zeroes, do we
        #    self.result = Decimal(self.result).quantize(DECIMAL_EXPONENT)
        self.result = Decimal(self.result).quantize(DECIMAL_EXPONENT)
        return self.result

    def __int__(self):
        return int(self.get_result())

    def __float__(self):
        return float(self.get_result())

    def __str__(self):
        return str(self.get_result())

    def __repr__(self):
        return '<lazy {name} by {values}>'.format(
            name = self.agg.name,
            values = '{0} values'.format(
                len(self.values) if len(self.values) > 3 else self.values
            )
        )


class NA(object):
    """
    Policy against N/A values. To be used in Aggregate constructors::

        Min('key', NA.skip).

    """
    skip, reject = 1, 2

    def __str__(self):
        return 'N/A'

    def __repr__(self):
        return '<N/A>'


class Aggregate(object):
    def __init__(self):
        self.key = None

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.key or 'all')

    def __repr__(self):
        return '<%s>' % str(self)

    def name(self):
        return self.__class__.__name__


class AggregateManager(Aggregate):
    "TODO factory?"

    def __init__(self, key, na_policy=NA.skip):
        self.key = key
        self.na_policy = na_policy

    def count_for(self, dictionaries):
        values = []
        for item in dictionaries:
            value = item.get(self.key, None)
            if value is None:
                # decide what to do if a None is found in values (i.e. a value is not available)
                if self.na_policy == NA.reject:
                    # reset the whole calculated value to None if at least one value is N/A
                    return None
                elif self.na_policy == NA.skip:
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
        return Decimal(sum(values, 0)) / len(values)


class Max(AggregateManager):
    @staticmethod
    def calc(values):
        return str(max(values))  # str for later conversion to decimal


class Median(AggregateManager):
    """
    Given a vector V of length N, the median of V is the middle value of a sorted
    copy of V, V_sorted - i.e., V_sorted[(N-1)/2], when N is odd. When N is even,
    it is the average of the two middle values of V_sorted.
    """
    @staticmethod
    def calc(values):
        # TODO: force Decimal
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
            _sum = sum(values[lower:upper])
            if isinstance(_sum, Decimal):
                return _sum / Decimal('2.0')
            else:
                return _sum / 2.0



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
    "Calculates the q0.25."
    def calc(self, values):
        values = sorted(values)
        l = len(values) / 4
        return super(Qu1, self).calc(values[:l])


class Qu3(Median):
    "Calculates the q0.75."
    def calc(self, values):
        values = sorted(values)
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
