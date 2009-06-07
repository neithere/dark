#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Runs a Python interactive interpreter.

Instantly accessible are:

1. `datashaping` for the datashaping package (a shortcut `ds` is also available);
2. `q` is a basic Query instance for given file. If none is specified, example
   data is loaded. Supported formats are YAML and JSON.

Example usage:

    $ python shell.py some_data.json
    (...greeting skipped...)
    >>> len(q)
    6712
    >>> ds.cast_cons(q, None, None, ds.Sum('price'))
    +------------+
    | Sum(price) |
    +------------+
    |   280383.0 |
    +------------+

"""

import os
import sys
import datashaping
import code
import readline
import rlcompleter

def _prepare_env(args):
    # read the file
    filename = sys.argv[1] if len(sys.argv) > 1 else 'tests/people.yaml'
    data     = _get_data(filename)
    dataset  = datashaping.db.Dataset(data)
    query    = datashaping.db.Query(dataset=dataset)
    imported_objects = {'datashaping': datashaping,
                        'ds': datashaping,  # shortcut
                        'q': query}
    print
    print "Available DataShape objects: `ds` (the module), `q` (the query)"
    print

    readline.set_completer(rlcompleter.Completer(imported_objects).complete)
    readline.parse_and_bind("tab:complete")

    code.interact(local=imported_objects)

def _get_data(filename):
    "Returns Python data read from given file."
    if not os.path.isfile(filename):
        raise ValueError, 'could not find file %s' % filename
    f = open(filename)
    loader = _get_loader(filename)
    return loader(f)

def _get_loader(filename):
    if filename.endswith('.yaml'):
        import yaml
        loader = yaml.load
    elif filename.endswith('.json'):
        import json
        loader = json.load
    else:
        raise ValueError, 'unknown data file type: %s' % filename
    return loader

if __name__ == '__main__':
    _prepare_env(sys.argv)
