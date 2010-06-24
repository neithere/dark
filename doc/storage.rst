Storage
=======

.. automodule:: dark.storage.memory
   :members:

Updating collections
--------------------

The package is primarily intended for data analysis which implies read-only
access to source data. There are, however, some cases when the power of queries
can be efficiently used to mutate large database dumps (e.g. JSON fixtures).

Adding and removing documents
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since v.0.3 items can be added or removed from storage on the fly. Indexing is
done per document, i.e. you don't have to run the whole indexing routine each
time you add or delete an item, the storage will automatically update only a
small part of the index according to item data.

Saving changed data is *not* supported. You can easily do it yourself, e.g.::

    >>> from dark.storage.memory import MemoryCollection
    >>> import json
    >>> file_in = open('in.json')
    >>> data = json.load(file_in)
    >>> storage = MemoryCollection(data)
    >>> storage.add({'foo': 'bar'})
    >>> file_out = open('out.json', 'w')
    >>> json.dump(storage.data, file_out)

There may be some quirks with changing the already indexed data. For example,
instances of :class:`~dark.query.Query` fetch data once and then cache
the results until are destroyed themselves. This means that already executed
queries will *not* be updated even if you empty the whole storage.

Editing documents
~~~~~~~~~~~~~~~~~

.. warning::

    :class:`~dark.query.Document` instances (and the underlying data
    items) are not immutable, but they do *not* keep storage index in sync with
    their contents.

If you happen to modify (not add/delete but edit) a field in a document, make
sure to rebuild the search index before doing queries on this field. When
the index goes out of sync, queries become unreliable and may return broken
data.

To "manually" rebuild storage index in a
:class:`~dark.storage.memory.MemoryCollection` instance you should call
its method `_build_index`. This operation is time-consuming, so you will not
want to perform it too frequently. While it is possible that in the future an
updated :class:`~dark.query.Document` instance would automatically
efficiently update the corresponding
:class:`~dark.storage.memory.MemoryCollection` index (remove/add parts),
currently it's not the case.

Also remember to manually save storage data to a file or DB after changing it.
