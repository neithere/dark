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
Discovery
=========

Tools to discover patterns in mixed data.

The module provides functions that allow to:

a) analyze the storage and extract high-level information such as field
   frequency and common structures;
b) pick best document class for a dictionary.

"""

from doqu import *


__all__ = [
    'suggest_document_class',
    'field_frequency', 'print_field_frequency',
    'suggest_structures', 'print_suggest_structures',
    'document_factory'
]


def suggest_document_class(data, classes, fit_whole_data=False,
                           require_schema=False):
    """
    Returns the best matching document class from given list of classes for
    given data dictionary. If no class matched the data, returns `None`.

    :param data:
        a dictionary.
    :param classes:
        a list of :class:`doqu.Document` subclasses.
    :param require_schema:
        If `True`, classes with empty schemata are discarded. Default is
        `False` because a document may have no schema but strict validators.
    :param fit_whole_data:
        if `True`, partial structural matches are discarded. Default is
        `False`, i.e. it is OK for the document class to only support a subset
        of fields found in the data dictionary.

    The guess is based on: a) how well does the declared structure match given
    dictionary, and b) does the resulting document validate or not. The classes
    are sorted by structure similarity and then the first valid choice is
    picked.
    """
    assert classes
    assert hasattr(classes, '__iter__')
    assert all(issubclass(cls, Document) for cls in classes)
    assert isinstance(data, dict)

    scores = {}
    for cls in classes:
        data_keys = set(data)
        schema_keys = set(cls.meta.structure)
        common_keys = schema_keys & data_keys
        if not common_keys:
            if schema_keys:
                continue  # wrong schema
            elif require_schema:
                continue  # empty schema -- but it's not allowed
        if fit_whole_data and data_keys - schema_keys:
            # not all data keys are present in the schema
            continue
        diff_keys = schema_keys ^ data_keys
        scores[cls] = len(common_keys) - len(diff_keys)

    candidates = sorted(scores.iteritems(), key=lambda x:x[1], reverse=True)
    for cls, score in candidates:
        instance = cls()  # for validation method
                          # TODO extract it to a standalone function?
        valid = True
        for field in cls.meta.structure:
            # TODO using private method; make it public?
            try:
                instance._validate_value(field, data.get(field))
            except validators.ValidationError:
                valid = False
                break
        if valid:
            # no validation errors for known fields, let's pick this class
            return cls
    return None

def suggest_structures(query, having=None):
    """
    Analyses all documents in given database and returns a list of unique
    structures found. The usefullness of the result depends on the database:
    for a highly irregular database a very large and almost useless list can be
    generated, while a more or less regular database can be easily inspected by
    this function.

    :param having:
        list of field names that must be present in each structure.

    Usage example::

        from doqu import *
        from dark import *

        db = get_db(backend='doqu.ext.tokyo_cabinet', path='foo.tct')
        for structure in suggest_structures(db):
            print structure
            doc_cls = document_factory(structure)
            print '  %d entries' % doc_cls.objects(db).count()

    See also :func:`print_suggest_structures`.
    """
    structures = {}  #[]
    for d in query:
        cols = tuple(sorted(d.keys()))
        if having and not all(k in cols for k in having):
            continue
        structures.setdefault(cols, 0)
        structures[cols] += 1
#        if cols in structures:
#            structures.append(cols)
    return sorted(structures.iteritems(), key=lambda x:x[1], reverse=True)

def print_suggest_structures(*args, **kwargs):
    """
    Prints nicely formatted output of :func:`suggest_structures`.
    """
    results = suggest_structures(*args, **kwargs)
    for structure, frequency in results:
        print u'×{frequency:>5} ... {structure}'.format(
            frequency=frequency, structure=', '.join(structure))

def field_frequency(query, having=None, raw=False):
    """
    Returns a list of pairs (field name, frequency) sorted by frequency in
    given query (most frequent field is listed first).

    See also :func:`print_field_frequency`.
    """
    freqs = {}
    for document in query:
        data = document._saved_state.data if raw else document
        if having and not all(k in data for k in having):
            continue
        for name in data.keys():
            freqs.setdefault(name, 0)
            freqs[name] += 1
    return sorted(freqs.iteritems(), key=lambda x:x[1], reverse=True)

def print_field_frequency(*args, **kwargs):
    """
    Prints nicely formatted output of :func:`field_frequency`.
    """
    results = field_frequency(*args, **kwargs)
    for name, frequency in results:
        print u'×{frequency:>5} ... {name}'.format(**locals())

def document_factory(structure, all_required=True):
    """
    Returns a :class:`doqu.document_base.Document` subclass for given
    structure including validators. Please note that each field will get the
    `unicode` data type. If you need a more precise

    :param structure:
        a list of keys. Any iterable will do. If it is a dictionary, only its
        keys will be used.
    :param all_required:
        If `True` (default), the all fields in the structure are considered
        mandatory and validator `Required` is added for each of them.

    Here's a use case. Say, we have a dictionary `data` and we need to find all
    documents with same fields::

        # make a document class with vaidators
        CustomDocument = document_factory(data)
        # the validators will generate a query
        similar_docs = CustomDocument.objects(storage)

    Note that this does not yield documents with *exactly* the same structure:
    it is only guaranteed that all fields present in `data` are also present in
    these records. Neither does this method guarantee that the data types would
    match.
    """
    # TODO: name it properly(?)
    class cls(Document):
        structure = dict.fromkeys(structure, unicode)
        validators = dict.fromkeys(structure, [validators.exists()])
    return cls
