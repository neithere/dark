Storage
=======

.. automodule:: datashaping.storage.memory
   :members:

Updating collections
--------------------

:class:`~datashaping.storage.memory.MemoryCollection` is primarily intended for
data analysis, this is why is does not provide any update/store methods.
Dictionaries are not immutable but are indexed and cached. If you edit the data
within a :class:`~datashaping.storage.memory.MemoryCollection` instance, the
index and cache will go out of sync. After an update you will have to abandon
all existing :class:`~datashaping.query.Query` objects because they fetch data
once and then cache the results until are destroyed themselves. You will also
have to rebuild :class:`~datashaping.storage.memory.MemoryCollection` index
manually by calling `MemoryCollection._build_index`. This operation is
time-consuming, so you will not want to perform it too frequently. While it is
possible that in the future an updated :class:`~datashaping.query.Document`
instance would automatically efficiently update the corresponding
:class:`~datashaping.storage.memory.MemoryCollection` index (remove/add parts),
currently it's not the case.
Also remember to manually save :attr:`~datashaping.storage.memory.MemoryCollection.data`
to a file or DB after changing it.
