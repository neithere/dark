# -*- coding: utf-8 -*-
#
#  Copyright (c) 2009 Andy Mikhailenko and contributors
#
#  This file is part of Datashaping.
#
#  Datashaping is free software under terms of the GNU Lesser
#  General Public License version 3 (LGPLv3) as published by the Free
#  Software Foundation. See the file README for copying conditions.
#

__all__ = ['BaseCollection']

''' BRAINSTORM

коллекции:
    в памяти:
        доступ к источнику:
            импорт из json/csv/yaml/pickle/notation3/dbm/shelve
            - определить формат (по имени файла или явно)
            - использовать соответствующий преобразователь (внеш. библиотеку + иногда доп. маппинг метаданных)
        структура данных:
            многомерная (коллекция = список словарей)
            (?) плоская (коллекция = список триплетов)  -- note: нужны также доп. абстракции для документов и поисковых механизмов
        ссылки:
            вряд ли связаны, скорее дублируются или есть какие-то "ручные" ссылки
            self-contained file, затруднительно как-то сослаться вовне, если только RDF
        метаданные:
            могут отсутствовать (json, yaml)
            могут определять имена ключей (csv: первая строка)
            могут определять primary key (couchdb)
            могут определять семантику (rdf, notation3, triplestore)
        особенности:
            ручное индексирование
            дешевые перебор, сопоставление
    внешние:
        доступ к источнику:
            соединение с СУБД (можно через sqlalchemy)
        структура данных:
            плоская (коллекция = таблица)
        ссылки:
            двусторонние связи с другими таблицами (коллекциями)
            полный набор коллекций известен
        метаданные:
            show tables (список коллекций)
            show create table (имена ключей, типы значений, исходящие ссылки)
        особенности:
            минимизировать число обращений
            дешевле выгрести больше одним запросом
документы:
    - универсальный (просто обертка для произвольного словаря)
    - с метаданными
        - контроль набора ключей
        - валидация значений
    - с поддержкой связей
        - внутри коллекции
        - между коллекциями (aka FKs)
            - через менеджер коллекций (aka database)
    - с каким-то поведением
        - триплет?
            - ссылки на ресурсы, коллекция = namespace -- через специальный менеджер распределенных коллекций?
менеджеры коллекций:
    - единичная коллекция
    - набор коллекций (база данных, конечное множество таблиц)
    - сеть коллекций (RDF namespaces, бесконечное множество ресурсов)

См. http://www.aminus.net/dejavu/chrome/common/doc/trunk/storage.html#other
'''

class BaseCollection(object):
    """
    Abstract class for collections.
    Subclasses must implement the basic query methods defined here.
    """
    def __init__(self, data):
        raise NotImplementedError

    #-------------------+
    #  Basic query API  |
    #-------------------+

    def add(self, item):
        raise NotImplementedError

    def delete(self, ids):
        raise NotImplementedError

    def fetch(self, ids):
        raise NotImplementedError

    def fetch_one(self, i):
        raise NotImplementedError

    def find_ids(self, *criteria):
        raise NotImplementedError

    def ids_by(self, key, value):
        raise NotImplementedError

    def inspect(self):
        raise NotImplementedError

    def keys(self):
	raise NotImplementedError

    def values_for(self, key, filter_by=None):
        raise NotImplementedError

class BaseCondition(object):
    """
    A query condition.

    :param lookup:
        a string composed of the key by which to search, and the desired
        lookup type. To match nested data structures compound keys can be
        used. Key parts are separated from each other and from the lookup type
        with lookup delimiter which is ``__`` (double underscore) by default.
        Lookup type determines *how* the comparison will be performed.
        See details below.
    :param value:
        makes sense in the context of given lookup type. In most cases
        it serves as example of how certain piece of data should look like.
        This is obviously not the case for boolean lookups such as ``filled``
        and ``exists``.
        If `value` is a string and in the compared data is of another type,
        `value` is coerced to that type. If this is not possible, TypeError
        is raised.
    :param negate:
        a boolean. If set to ``True``, the criterion's meaning is inverted.
        Defaults to ``False``.

    If lookup type is not specified, ``exact`` is chosen by default.

    If `value` is empty (e.g. ``foo=None``), lookup type ``exact`` is
    replaced by ``filled``. This does not change the meaning but enables
    more compact index table. Please note that lookup types ``filled``
    and ``exists`` are different.

    Examples of lookups:

        ``foo``
            key ``foo``, lookup type ``exact``
        ``foo__exact``
            key ``foo``, lookup type ``exact``
        ``foo__bar``
            key ``foo``, nested key ``bar``, lookup type ``exact``
        ``foo__bar__exact``
            key ``foo``, nested key ``bar``, lookup type ``exact``
        ``foo__in``
            key ``foo``, lookup type ``in``
        ``foo__bar__in``
            key ``foo``, nested key ``bar``, lookup type ``in``

    Lookup types:

        * ``exact`` -- item has exactly the same value for given key. This lookup
            does not involve item-by-item comparison and is thus very fast;
        * ``not`` -- negated ``exact``;
        * ``gt``
        * ``lt``
        * ``gte``
        * ``lte``
        * ``in`` -- item value is a subset of ``value``;
        * ``contains`` -- ``value`` is a subset of item value;
        * ``filled`` -- item has given key, value is not empty (not None);
        * ``exists`` -- item has given key, value can be None.

        Date-related lookup types:

        * ``year``
        * ``month``
        * ``day``
        * ``week_day``

    """

    # LOOKUP_DELIMITER is a delimiter for indexing and subsequent lookups, e.g.:
    # "foo__bar" matches {'foo':{'bar':123}}.
    # The delimiter can be set to "__" or whatever in order to allow lookups
    # by keys containing a dot.
    # Note that this will be used as query.find(foo__bar=123), so a little subset
    # of symbols is allowed here.
    #
    # XXX this should be automatically controlled: if current delimiter is found
    #     within keys, another must be auto-chosen and data reindexed.

    LOOKUP_DELIMITER = '__'

    LOOKUP_TYPES = {
        'exact':    lambda value, other:  value == other, # normally direct lookup instead
        'not':      lambda value, other:  value != other,
        'gt':       lambda value, other:  value <  other,
        'lt':       lambda value, other:  other <  value,
        'gte':      lambda value, other:  value <= other,
        'lte':      lambda value, other:  other <= value,
        'in':       lambda value, other:  other in value,  # TODO: date range
        'contains': lambda value, other:  value in other,
        'filled':   lambda value, other:  other is not None,  # defined and not None
        'exists':   lambda value, other:  True,    # any value is OK if it's defined
        # dates:
        'year':     lambda value, other:  value == other.year,
        'month':    lambda value, other:  value == other.month,
        'day':      lambda value, other:  value == other.day,
        'week_day': lambda value, other:  value == other.weekday(),
    }

    def __init__(self, lookup, value, negate=False):
        self.lookup = lookup
        self.value  = value
        self.negate = negate
        self._key  = None
        self._type = None

    @property
    def key(self):
        if not self._key: self._resolve_lookup()
        return self._key

    @property
    def lookup_type(self):
        if not self._type: self._resolve_lookup()
        return self._type

    def _resolve_lookup(self):
        "Defines real lookup key and lookup type for given lookup key."
        # FIXME pubdate__month__gt -- nested lookup types; not necessarily indexed
        parts = self.lookup.split(self.LOOKUP_DELIMITER)
        if len(parts) > 1 and parts[-1] in self.LOOKUP_TYPES:
            lookup_type = parts.pop()
            self._key  = self.LOOKUP_DELIMITER.join(parts)
            self._type = lookup_type
        else:
            self._key  = self.lookup
            self._type = 'exact'
