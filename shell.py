#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Runs a Python interactive interpreter.

Instantly accessible are:

1. `datashaping` for the datashaping package (a shortcut `ds` is also available);
2. `q` is a basic Query instance for given file. If none is specified, example
   data is loaded. Supported formats are YAML and JSON.

Example usage:

    $ ./shell.py -f some_data.json
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

def _prepare_env(options):
    # read the file
    
    filename = options.filename or 'tests/people.yaml'
    data     = _get_data(filename)
    storage  = datashaping.storage.memory.MemoryCollection(data)
    query    = datashaping.query.Query(storage=storage)

    print
    print "Welcome to DataShaping v.%s interactive console." % datashaping.__version__
    print "Available objects: `ds` (datashaping module), `q` (query)."
    print

    namespace = {'datashaping': datashaping,
                 'ds': datashaping,  # shortcut
                 'q': query}

    # The code below is based on django.core.management.commands.shell:
    try:
        if not options.use_plain:
            raise ImportError
        import IPython
        shell = IPython.Shell.IPShell(user_ns=namespace)
        shell.mainloop()
    except ImportError:
        try:
            import readline
        except ImportError:
            pass
        else:
            import rlcompleter
            readline.set_completer(rlcompleter.Completer(namespace).complete)
            readline.parse_and_bind("tab:complete")
        # We want to honor both $PYTHONSTARTUP and .pythonrc.py, so follow system
        # conventions and get $PYTHONSTARTUP first then import user.
        if not options.use_plain:
            pythonrc = os.environ.get("PYTHONSTARTUP")
            if pythonrc and os.path.isfile(pythonrc):
                try:
                    execfile(pythonrc)
                except NameError:
                    pass
            # This will import .pythonrc.py as a side-effect
            import user

        code.interact(local=namespace)

def _get_data(filename):
    "Returns Python data read from given file."
    if not os.path.isfile(filename):
        raise ValueError, 'could not find file %s' % filename
    f = open(filename)
    loader = _get_loader(filename)
    print 'Loading file %s...' % filename
    return loader(f)

def _load_csv(file):
    import csv
    reader = csv.reader(file)
    meta = reader.next()
    data = []
    for row in reader:
        doc = {}
        for i, cell in enumerate(row):
            doc[meta[i]] = cell
        data.append(doc)
    return data

def _get_loader(filename):
    # XXX add SQL loaders; this implies switching storage modules, too
    if filename.endswith('.yaml'):
        import yaml
        loader = yaml.load
    elif filename.endswith('.json'):
        import json
        loader = json.load
    elif filename.endswith('.csv'):
        loader = _load_csv
    else:
        raise ValueError, 'unknown data file type: %s' % filename
    return loader

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser(version='%%prog %s' % datashaping.__version__)
    parser.add_option('-f', '--file', dest='filename', metavar='FILE',
                      help='Load data from FILE.')
    parser.add_option('-p', '--plain',  action='store_true', dest='use_plain',
                      help='Tells Django to use plain Python, not IPython.')
    (options, args) = parser.parse_args()
    _prepare_env(options)
