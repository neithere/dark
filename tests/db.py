# -*- coding: utf-8 -*-

"""
>>> import datetime
>>> from datashaping.db import Dataset
>>> from datashaping.aggregates import Avg, Count
>>> import yaml
>>> data = yaml.load(open('tests/people.yaml'))
>>> people = Dataset(data)
>>> people.inspect() == {'website': 3, 'born': 18, 'name': 18, 'nick': 4,
...                      'age': 16, 'gender': 16, 'occupation': 16,
...                      'fullname': 8, 'residence': 1}
True
>>> len(people.all())   # results are found
18
>>> len(people.all())   # iterator is not exhausted
18
>>> people.find()[3]
<Document 3>

# "exists" lookup type

>>> len(people.all().find(website__exists=True))
3
>>> len(people.all().exclude(website__exists=True))
15
>>> len(people.all().find(website__exists=False))
15
>>> len(people.all().exclude(website__exists=False))
3
>>> len(people.all().find(nick__exists=True))
5
>>> len(people.all().find(website__exists=True, nick__exists=True))
1
>>> len(people.all().find(website__exists=True, nick__exists=False))
2
>>> len(people.all().find(website__exists=False, nick__exists=True))
4
>>> len(people.all().find(website__exists=False, nick__exists=False))
11
>>> len(people.all().find(website__exists=True).find(nick__exists=True))
1
>>> len(people.all().find(website__exists=True).exclude(nick__exists=True))
2
>>> len(people.all().find(website__exists=True).exclude(nick__exists=True).find(born__country='Finland'))
1


# "filled" lookup type

>>> len(people.all().find(nick__filled=True))
4
>>> len(people.all().find(nick__filled=False))
14
>>> len(people.all().find(nick__exists=True, nick__filled=False))
1
>>> len(people.all().exclude(nick__filled=True))
14




>>> len(people.find(born__city__not=None))
17
>>> len(people.all().exclude(born__city=None))     # same as previous, different syntax
17
>>> people.all().exclude(born__city=None).count()  # same as previous, yet another syntax
17
>>> people.find(born__country='England')
[<Document 0>, <Document 2>]
>>> repr(people.find(born__country='England'))
'[<Document 0>, <Document 2>]'
>>> [p.name for p in people.find(born__country='England')]
['Thomas Fowler', 'Alan Turing']
>>> len(people.find(born__country__not='USA'))
11
>>> len(people.find(born__country='USA')) + len(people.find(born__country='England'))
9
>>> len(people.all().exclude(born__country__in=['USA','England']))
9
>>> [p.name for p in people.find(born__country='USA', gender__not='male')]    # multiple conditions
['Anita Borg', 'Kathleen Antonelli', 'Jean Bartik']
>>> [p.name for p in people.find(born__country='USA').exclude(gender='male')]  # same but cleaner
['Anita Borg', 'Kathleen Antonelli', 'Jean Bartik']
>>> [(p.name,p.age) for p in people.find(age__lt=50)]
[('Guido van Rossum', 49), ('Theo de Raadt', 41), ('Linus Torvalds', 40)]
>>> [(p.name,p.age) for p in people.find(age__gt=100)]
[('Thomas Fowler', 232), ('Conrad Palm', 102)]
>>> item = people.all()[11]
>>> item
<Document 11>
>>> item._dict == None   # empty dict
True
>>> item.name
'Richard Stallman'
>>> item._dict == {'name': 'Richard Stallman',
...                'fullname': {'first':  'Richard',
...                             'middle': 'Matthew',
...                             'last':   'Stallman'},
...                'nick': 'rms',
...                'age': 56,
...                'gender': 'male',
...                'born': {'date':    datetime.date(1953, 3, 16),
...                         'country': 'USA',
...                         'city':    'New York'},
...                'occupation': 'President of the FSF',
...                'website': 'http://stallman.org'}
True
>>> people.values_for('born__country')
['England', 'Finland', 'Netherlands', 'New Zealand', 'Norway', 'South Africa', 'Sweden', 'Switzerland', 'USA']

# group by country
>>> for country in people.values_for('born__country'):
...     print country
England
Finland
Netherlands
New Zealand
Norway
South Africa
Sweden
Switzerland
USA

# group by country; calculate average age
>>> for country in people.values_for('born__country'):
...     documents = people.find(born__country=country)
...     print country, str(Avg('age').count_for(documents))
England 164.5
Finland 40.0
Netherlands 64.0
New Zealand N/A
Norway 83.0
South Africa 41.0
Sweden 102.0
Switzerland 75.0
USA 67.7142857143

# group by country; count country population
>>> for country in sorted(people.values_for('born__country')):
...     documents = people.find(born__country=country)
...     print country, int(Count().count_for(documents))
England 2
Finland 1
Netherlands 2
New Zealand 2
Norway 1
South Africa 1
Sweden 1
Switzerland 1
USA 7

# group by country and city
>>> for country in people.values_for('born__country'):
...     for city in people.find(born__country=country).values_for('born__city'):
...         print country, '-', city
England - Great Torrington
England - Maida Vale, London
Finland - Helsinki
Netherlands - Amsterdam
Netherlands - Rotterdam
New Zealand - Auckland
Norway - Oslo
South Africa - Pretoria
Sweden - Stockholm
Switzerland - Winterthur
USA - None
USA - Betty Jean Jennings
USA - Chicago
USA - Milwaukee
USA - New York
USA - San Jose
USA - Seattle

# group by country and city; count city population
>>> for country in people.values_for('born__country'):
...     for city in people.find(born__country=country).values_for('born__city'):
...         documents = people.find(born__country=country, born__city=city)
...         print '%s, %s (%d)' % (country, city, Count().count_for(documents))
England, Great Torrington (1)
England, Maida Vale, London (1)
Finland, Helsinki (1)
Netherlands, Amsterdam (1)
Netherlands, Rotterdam (1)
New Zealand, Auckland (2)
Norway, Oslo (1)
South Africa, Pretoria (1)
Sweden, Stockholm (1)
Switzerland, Winterthur (1)
USA, None (1)
USA, Betty Jean Jennings (1)
USA, Chicago (1)
USA, Milwaukee (1)
USA, New York (1)
USA, San Jose (1)
USA, Seattle (1)

# group by country, city and gender
>>> for country in people.values_for('born__country'):
...     for city in people.find(born__country=country).values_for('born__city'):
...         for gender in people.find(born__country=country, born__city=city).values_for('gender'):
...             print '%s, %s, %s' % (country, city, gender)
England, Great Torrington, male
England, Maida Vale, London, male
Finland, Helsinki, male
Netherlands, Amsterdam, male
Netherlands, Rotterdam, male
New Zealand, Auckland, male
Norway, Oslo, male
Sweden, Stockholm, male
Switzerland, Winterthur, male
USA, None, female
USA, Betty Jean Jennings, female
USA, Chicago, female
USA, Milwaukee, male
USA, New York, male
USA, San Jose, male
USA, Seattle, male

# group by country, city, and gender; calculate average age
>>> for country in people.values_for('born__country'):
...     for city in people.find(born__country=country).values_for('born__city'):
...         for gender in people.find(born__country=country, born__city=city).values_for('gender'):
...             documents = people.find(born__country=country, born__city=city, gender=gender)
...             print 'Average %s from %s, %s is %s years old' % (gender, city, country, str(Avg('age').count_for(documents)))
Average male from Great Torrington, England is 232.0 years old
Average male from Maida Vale, London, England is 97.0 years old
Average male from Helsinki, Finland is 40.0 years old
Average male from Amsterdam, Netherlands is 49.0 years old
Average male from Rotterdam, Netherlands is 79.0 years old
Average male from Auckland, New Zealand is N/A years old
Average male from Oslo, Norway is 83.0 years old
Average male from Stockholm, Sweden is 102.0 years old
Average male from Winterthur, Switzerland is 75.0 years old
Average female from None, USA is 88.0 years old
Average female from Betty Jean Jennings, USA is 85.0 years old
Average female from Chicago, USA is 60.0 years old
Average male from Milwaukee, USA is 72.0 years old
Average male from New York, USA is 56.0 years old
Average male from San Jose, USA is 59.0 years old
Average male from Seattle, USA is 54.0 years old

#----------------+
# Nested structs |
#----------------+

# values for nested key

>>> people.values_for('fullname__first')
['Alan', 'Anita', 'Donald', 'Guido', 'Linus', 'Richard', 'Stephen', 'Theo']

# nested lookup

>>> [p.name for p in people.find(fullname__last='Torvalds')]
['Linus Torvalds']

# simple lookup, then values for nested key

>>> people.find(gender='male').values_for('fullname__first')
['Alan', 'Donald', 'Guido', 'Linus', 'Richard', 'Stephen']

# nested lookup, then values for nested key

>>> people.find(born__country='USA').values_for('fullname__first')
['Anita', 'Donald', 'Richard', 'Stephen']

# nested lookup, then values for simple key

>>> [p.name for p in people.find(born__country='USA').exclude(gender='male')]
['Anita Borg', 'Kathleen Antonelli', 'Jean Bartik']

#>>> results = people.all().group_by('born__country', 'born__city').order_by('age')
#>>> results
#{ ('USA', 'New York'): [<Document 1>, <Document 2>]
#}

#-----------------------+
# Dataset._unwrap_value |
#-----------------------+

>>> d = Dataset([])
>>> x = d._unwrap_value('foo', 'bar')
>>> dict(x) == dict([('foo', 'bar')])
True
>>> x = d._unwrap_value('foo', ['bar','quux'])
>>> dict(x) == dict([('foo', 'bar'),
...                  ('foo', 'quux')])
True
>>> x = d._unwrap_value('foo', {'bar': 'quux'})
>>> dict(x) == dict([('foo__bar', 'quux')])
True
>>> x = d._unwrap_value('foo', {'bar': [123, 456]})
>>> dict(x) == dict([('foo__bar', 123),
...                  ('foo__bar', 456)])
True
>>> x = d._unwrap_value('foo', {'bar': {'quux': 123}})
>>> dict(x) == dict([('foo__bar__quux', 123)])
True
>>> x = d._unwrap_value('foo', {'bar': {'quux': [123, 456]}})
>>> dict(x) == dict([('foo__bar__quux', 123),
...                  ('foo__bar__quux', 456)])
True
>>> x = d._unwrap_value('foo', {'bar': [{'quux': [123, 456], 'quack': 789}]})
>>> dict(x) == dict([('foo__bar__quux', 123),
...                  ('foo__bar__quux', 456),
...                  ('foo__bar__quack', 789)])
True
>>> x = d._unwrap_value('foo', datetime.date(2009, 6, 2))
>>> dict(x) == dict([('foo', datetime.date(2009, 6, 2)),
...                  ('foo__year', 2009),
...                  ('foo__month', 6),
...                  ('foo__day', 2)])
True

# >>> x = d._unwrap_value('foo', datetime.datetime(2009, 6, 2, 16, 42))
# >>> dict(x) == dict([('foo', datetime.datetime(2009, 6, 2, 16, 42)),
# ...                  ('foo__year', 2009),
# ...                  ('foo__month', 6),
# ...                  ('foo__day', 2),
# ...                  ('foo__hour', 16),
# ...                  ('foo__minute', 42)])
# True

"""
