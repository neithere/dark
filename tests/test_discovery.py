# -*- coding: utf-8 -*-

import unittest

from docu import Document
from dark.discovery import guess_doc_class


class ClassGuessTestCase(unittest.TestCase):

    def setUp(self):
        class Person(Document):
            structure = {'name': unicode}

        class User(Document):
            structure = {'name': unicode, 'password': unicode}

        self.person_class = Person
        self.user_class = User
        self.choices = Person, User

    def tearDown(self):
        pass

    def test_guess_person(self):
        "Person"
        data = {'name': u'John'}
        cls = guess_doc_class(data, self.choices)
        self.assertEquals(cls, self.person_class)

    def test_guess_person_invalid(self):
        "Person (invalid data type)"
        data = {'name': 123}
        cls = guess_doc_class(data, self.choices)
        self.assertEquals(cls, None)

    def test_guess_user(self):
        "User"
        data = {'name': u'John', 'password': u'foo'}
        cls = guess_doc_class(data, self.choices)
        self.assertEquals(cls, self.user_class)

    def test_guess_none(self):
        "Unknown structure"
        data = {'foo': 'bar'}
        cls = guess_doc_class(data, self.choices)
        self.assertEquals(cls, None)
