# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009—2010 Andy Mikhailenko and contributors
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


__all__ = ['suggest_structures', 'document_factory']


class CatchAll(Document):
    pass


def suggest_structures(db):
    """
    Analyses all documents in given database and returns a list of unique
    structures found. The usefullness of the result depends on the database:
    for a highly irregular database a very large and almost useless list can be
    generated, while a more or less regular database can be easily inspected by
    this function.
    """
    # TODO: same but query-based
    structures = []
    for d in CatchAll.objects(db):
        cols = sorted(d.keys())
        if cols not in structures:
            structures.append(cols)
    return structures

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
