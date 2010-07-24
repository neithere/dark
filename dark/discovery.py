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

Analyzes a mixed dataset and suggests document structures.

Usage example::

    from docu import *
    from dark import *

    db = get_db(backend='docu.ext.tokyo_cabinet', path='foo.tct')
    for structure in suggest_structures(db):
        print structure
        doc_cls = document_factory(structure)
        print '  %d entries' % doc_cls.objects(db).count()

"""

from docu import *
from docu.validators import exists


__all__ = [
    'field_frequency', 'print_field_frequency',
    'suggest_structures', 'print_suggest_structures',
    'document_factory'
]



def suggest_structures(query, having=None):
    """
    Analyses all documents in given database and returns a list of unique
    structures found. The usefullness of the result depends on the database:
    for a highly irregular database a very large and almost useless list can be
    generated, while a more or less regular database can be easily inspected by
    this function.

    :param having:
        list of field names that must be present in each structure.

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
    results = suggest_structures(*args, **kwargs)
    for structure, frequency in results:
        print u'×{frequency:>5} ... {structure}'.format(
            frequency=frequency, structure=', '.join(structure))

def field_frequency(query, having=None, raw=False):
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
    results = field_frequency(*args, **kwargs)
    for name, frequency in results:
        print u'×{frequency:>5} ... {name}'.format(**locals())

def document_factory(structure):
    """
    Returns a :class:`docu.document_base.Document` subclass for given
    structure including validators.
    """
    # TODO: name it properly(?)
    class cls(Document):
        structure = dict.fromkeys(structure, unicode)
        validators = dict.fromkeys(structure, [exists()])
    return cls
