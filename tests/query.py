# -*- coding: utf-8 -*-

"""
>>> import datetime
>>> from dark.query import Query
>>> from dark.storage.memory import MemoryCollection
>>> from dark.aggregates import Avg, Count
>>> import yaml
>>> data = yaml.load(open('tests/people.yaml'))
>>> storage = MemoryCollection(data)
>>> people = Query(storage=storage)
>>> len(people)   # results are found
18
>>> len(people)   # iterator is not exhausted
18
>>> people.find()[3]
<Document 3>

# ordering

>>> [d.get('gender') for d in people]
['male', 'male', 'male', 'female', 'female', 'male', 'male', 'male', 'male', 'female', 'male', 'male', 'male', 'male', 'male', None, 'male', None]
>>> [d.get('gender') for d in people.order_by('gender')]
[None, None, 'female', 'female', 'female', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male', 'male']
>>> [d.get('age') for d in people.order_by('age')]
[None, None, 40, 41, 49, 54, 56, 59, 60, 72, 75, 79, 83, 85, 88, 97, 102, 232]
>>> [d.get('age') for d in people.order_by('-age')]
[232, 102, 97, 88, 85, 83, 79, 75, 72, 60, 59, 56, 54, 49, 41, 40, None, None]
>>> [d._pk for d in people.order_by('gender', 'age')]
[15, 14, 13, 17, 16, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
>>> [d._pk for d in people.order_by('gender', '-age')]
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 13, 15, 14]

# "exists" lookup type

>>> len(people.find(website__exists=True))
3
>>> len(people.exclude(website__exists=True))
15
>>> len(people.find(website__exists=False))
15
>>> len(people.exclude(website__exists=False))
3
>>> len(people.find(nick__exists=True))
5
>>> len(people.find(website__exists=True, nick__exists=True))
1
>>> len(people.find(website__exists=True, nick__exists=False))
2
>>> len(people.find(website__exists=False, nick__exists=True))
4
>>> len(people.find(website__exists=False, nick__exists=False))
11
>>> len(people.find(website__exists=True).find(nick__exists=True))
1
>>> len(people.find(website__exists=True).exclude(nick__exists=True))
2
>>> len(people.find(website__exists=True).exclude(nick__exists=True).find(born__country='Finland'))
1

# "filled" lookup type

>>> len(people.find(nick__filled=True))
4
>>> len(people.find(nick__filled=False))
14
>>> len(people.find(nick__exists=True, nick__filled=False))
1
>>> len(people.exclude(nick__filled=True))
14


>>> len(people.find(born__city__not=None))
17
>>> len(people.exclude(born__city=None))     # same as previous, different syntax
17
>>> people.exclude(born__city=None).count()  # same as previous, yet another syntax
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
>>> len(people.exclude(born__country__in=['USA','England']))
9
>>> [p.name for p in people.find(born__country='USA', gender__not='male')]    # multiple conditions
['Anita Borg', 'Kathleen Antonelli', 'Jean Bartik']
>>> [p.name for p in people.find(born__country='USA').exclude(gender='male')]  # same but cleaner
['Anita Borg', 'Kathleen Antonelli', 'Jean Bartik']
>>> [(p.name,p.age) for p in people.find(age__lt=50)]
[('Guido van Rossum', 49), ('Theo de Raadt', 41), ('Linus Torvalds', 40)]
>>> [(p.name,p.age) for p in people.find(age__gt=100)]
[('Thomas Fowler', 232), ('Conrad Palm', 102)]
>>> item = people[11]
>>> item._data == {'name': 'Richard Stallman',
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
>>> item.name
'Richard Stallman'
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

#>>> results = people.group_by('born__country', 'born__city').order_by('age')
#>>> results
#{ ('USA', 'New York'): [<Document 1>, <Document 2>]
#}

"""
