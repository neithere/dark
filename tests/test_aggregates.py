# -*- coding: utf-8 -*-

import docu
import os
import unittest
import yaml

from dark.aggregates import Avg, Count, Max, Median, Min, NA, Qu1, Qu3, Sum


TMP_DB_PATH = '_test_aggregates.shelve'


class Person(docu.Document):
    def __unicode__(self):
        return u'{first_name} {last_name}'.format(
            first_name = self.get('first_name', '?'),
            last_name = self.get('last_name', '?'),
        )


class AggregatesTestCase(unittest.TestCase):


    def setUp(self):
        assert not os.path.exists(TMP_DB_PATH), (
            'Sandbox file {0} must not exist'.format(os.path.abspath(TMP_DB_PATH)))
        self.db = docu.get_db(backend='docu.ext.shelve_db', path=TMP_DB_PATH)
        for data in yaml.load(open('tests/people.yaml')):
            Person(**data).save(self.db)
        self.people = Person.objects(self.db)

    def tearDown(self):
        self.db.connection.close()
        os.unlink(TMP_DB_PATH)

    def test_all_aggregates(self):
        "All aggregates"
        people = self.people
        assert int(Count().count_for(people)) == 18
        assert int(Avg('age').count_for(people)) == 79
        assert int(Min('age').count_for(people)) == 40
        assert int(Max('age').count_for(people)) == 232
        assert int(Sum('age').count_for(people)) == 1272
        assert int(Qu1('age').count_for(people)) == 45
        assert int(Qu3('age').count_for(people)) == 99

    def test_na_policy(self):
        "N/A policy"
        people = self.people
        # below we coerce lazy calculations to int
        assert int(Sum('age').count_for(people)) == 1272
        assert int(Sum('age', NA.skip).count_for(people)) == 1272
        assert Sum('age', NA.reject).count_for(people) is None
        assert int(Min('age').count_for(people)) == 40
        assert int(Min('age', NA.skip).count_for(people)) == 40
        assert Min('age', NA.reject).count_for(people) is None

    def test_avg(self):
        "Calculating average"
        rows = [{'x': 0.5}, {'x': 1.5}]
        assert float(Avg('x').count_for(rows)) == 1.0
