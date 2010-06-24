# -*- coding: utf-8 -*-

"""
>>> from dark.query import Query
>>> from dark.storage.memory import MemoryCollection
>>> from dark.aggregates import Avg, Count, Max, Median
>>> from dark.shaping import cast_cons, stdev, summary
>>> import yaml
>>> data = yaml.load(open('tests/people.yaml'))
>>> q = Query(storage=MemoryCollection(data))

# note: function cast_cons() is a simple wrapper for cast(), prints a basic table

# count, no grouper factors, implicit aggregate

>>> cast_cons(q)
 +------------+
 | Count(all) |
 +------------+
 |         18 |
 +------------+

# count, implicit aggregate

>>> cast_cons(q, ['gender'])
 +--------+------------+
 | gender | Count(all) |
 +--------+------------+
 | female |          3 |
 |   male |         13 |
 +--------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['gender'], [], Count())
 +--------+------------+
 | gender | Count(all) |
 +--------+------------+
 | female |          3 |
 |   male |         13 |
 +--------+------------+

>>> cast_cons(q, ['gender'], [], Count('nick'))
 +--------+-------------+
 | gender | Count(nick) |
 +--------+-------------+
 | female |           1 |
 |   male |           3 |
 +--------+-------------+

>>> cast_cons(q, [], ['gender'], Count('nick'))
 +--------+------+-------------+
 | female | male | Count(nick) |
 +--------+------+-------------+
 |      1 |    3 |           4 |
 +--------+------+-------------+

>>> cast_cons(q, [], ['gender'], Count())
 +--------+------+------------+
 | female | male | Count(all) |
 +--------+------+------------+
 |      3 |   13 |         18 |
 +--------+------+------------+

>>> cast_cons(q, [], ['born__country'], Count('nick'))
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+
 | England | Finland | Netherlands | New Zealand | Norway | South Africa | Sweden | Switzerland | USA | Count(nick) |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+
 |     N/A |     N/A |         N/A |         N/A |    N/A |          N/A |      1 |         N/A |   3 |           4 |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+-------------+

# count, implicit aggregate

>>> cast_cons(q, ['born__country'])
 +---------------+------------+
 | born__country | Count(all) |
 +---------------+------------+
 |       England |          2 |
 |       Finland |          1 |
 |   Netherlands |          2 |
 |   New Zealand |          2 |
 |        Norway |          1 |
 |  South Africa |          1 |
 |        Sweden |          1 |
 |   Switzerland |          1 |
 |           USA |          7 |
 +---------------+------------+

# count, explicit aggregate

>>> cast_cons(q, ['born__country'], [], Count())
 +---------------+------------+
 | born__country | Count(all) |
 +---------------+------------+
 |       England |          2 |
 |       Finland |          1 |
 |   Netherlands |          2 |
 |   New Zealand |          2 |
 |        Norway |          1 |
 |  South Africa |          1 |
 |        Sweden |          1 |
 |   Switzerland |          1 |
 |           USA |          7 |
 +---------------+------------+

# count, multiple factors (two)

>>> cast_cons(q, ['born__country','born__city'], [], Count())
 +---------------+---------------------+------------+
 | born__country |          born__city | Count(all) |
 +---------------+---------------------+------------+
 |       England |    Great Torrington |          1 |
 |       England |  Maida Vale, London |          1 |
 |       Finland |            Helsinki |          1 |
 |   Netherlands |           Amsterdam |          1 |
 |   Netherlands |           Rotterdam |          1 |
 |   New Zealand |            Auckland |          2 |
 |        Norway |                Oslo |          1 |
 |  South Africa |            Pretoria |          1 |
 |        Sweden |           Stockholm |          1 |
 |   Switzerland |          Winterthur |          1 |
 |           USA |                None |          1 |
 |           USA | Betty Jean Jennings |          1 |
 |           USA |             Chicago |          1 |
 |           USA |           Milwaukee |          1 |
 |           USA |            New York |          1 |
 |           USA |            San Jose |          1 |
 |           USA |             Seattle |          1 |
 +---------------+---------------------+------------+

# explicit custom aggregate, no grouper factor

>>> cast_cons(q, [], [], Avg('age'))
 +----------+
 | Avg(age) |
 +----------+
 |     79.5 |
 +----------+

>>> cast_cons(q, [], [], Median('age'))
 +-------------+
 | Median(age) |
 +-------------+
 |        73.5 |
 +-------------+

# aggregate by one key, Avg

>>> cast_cons(q, ['born__country'], [], Avg('age'))
 +---------------+----------+
 | born__country | Avg(age) |
 +---------------+----------+
 |       England |    164.5 |
 |       Finland |     40.0 |
 |   Netherlands |     64.0 |
 |   New Zealand |      N/A |
 |        Norway |     83.0 |
 |  South Africa |     41.0 |
 |        Sweden |    102.0 |
 |   Switzerland |     75.0 |
 |           USA |     67.7 |
 +---------------+----------+

# aggregate by one key, Max

>>> cast_cons(q, ['born__country'], [], Max('age'))
 +---------------+----------+
 | born__country | Max(age) |
 +---------------+----------+
 |       England |      232 |
 |       Finland |       40 |
 |   Netherlands |       79 |
 |   New Zealand |      N/A |
 |        Norway |       83 |
 |  South Africa |       41 |
 |        Sweden |      102 |
 |   Switzerland |       75 |
 |           USA |       88 |
 +---------------+----------+

# aggregate by multiple keys

>>> cast_cons(q, ['born__country','born__city','gender'], [], Avg('age'))
 +---------------+---------------------+--------+----------+
 | born__country |          born__city | gender | Avg(age) |
 +---------------+---------------------+--------+----------+
 |       England |    Great Torrington |   male |    232.0 |
 |       England |  Maida Vale, London |   male |     97.0 |
 |       Finland |            Helsinki |   male |     40.0 |
 |   Netherlands |           Amsterdam |   male |     49.0 |
 |   Netherlands |           Rotterdam |   male |     79.0 |
 |   New Zealand |            Auckland |   male |      N/A |
 |        Norway |                Oslo |   male |     83.0 |
 |  South Africa |            Pretoria |   None |     41.0 |
 |        Sweden |           Stockholm |   male |    102.0 |
 |   Switzerland |          Winterthur |   male |     75.0 |
 |           USA |                None | female |     88.0 |
 |           USA | Betty Jean Jennings | female |     85.0 |
 |           USA |             Chicago | female |     60.0 |
 |           USA |           Milwaukee |   male |     72.0 |
 |           USA |            New York |   male |     56.0 |
 |           USA |            San Jose |   male |     59.0 |
 |           USA |             Seattle |   male |     54.0 |
 +---------------+---------------------+--------+----------+

# multiple aggregates -- NOT YET

#>>> cast_cons(q, ['born__country','born__city'], [], [Avg('age'), Min('age'), Max('age')])

# city key not present, level empty, use higher level query

>>> cast_cons(q.find(nick='Kay'), ['name', 'born__country', 'born__city'])
 +--------------------+---------------+------------+------------+
 |               name | born__country | born__city | Count(all) |
 +--------------------+---------------+------------+------------+
 | Kathleen Antonelli |           USA |       None |          1 |
 +--------------------+---------------+------------+------------+

# city key not present, level empty, use higher level query

>>> cast_cons(q.find(nick='Kay'), ['born__city'])
 +------------+------------+
 | born__city | Count(all) |
 +------------+------------+
 |       None |          1 |
 +------------+------------+

# PIVOTING

# pivoting by one factor

>>> cast_cons(q, ['born__country'], ['gender'])
 +---------------+--------+------+------------+
 | born__country | female | male | Count(all) |
 +---------------+--------+------+------------+
 |       England |      0 |    2 |          2 |
 |       Finland |      0 |    1 |          1 |
 |   Netherlands |      0 |    2 |          2 |
 |   New Zealand |      0 |    1 |          2 |
 |        Norway |      0 |    1 |          1 |
 |  South Africa |      0 |    0 |          1 |
 |        Sweden |      0 |    1 |          1 |
 |   Switzerland |      0 |    1 |          1 |
 |           USA |      3 |    4 |          7 |
 +---------------+--------+------+------------+

# pivoting by multiple factors, one level

>>> cast_cons(q.find(born__country='England'), ['born__country'], ['born__city', 'gender'])
 +---------------+------------------+--------------------+------+------------+
 | born__country | Great Torrington | Maida Vale, London | male | Count(all) |
 +---------------+------------------+--------------------+------+------------+
 |       England |                1 |                  1 |    2 |          2 |
 +---------------+------------------+--------------------+------+------------+

# pivoting by multiple factors, multiple levels

>>> cast_cons(q.find(born__country__in=['Netherlands','Finland']), ['born__country'], ['born__city', 'gender'])
 +---------------+-----------+----------+-----------+------+------------+
 | born__country | Amsterdam | Helsinki | Rotterdam | male | Count(all) |
 +---------------+-----------+----------+-----------+------+------------+
 |       Finland |         0 |        1 |         0 |    1 |          1 |
 |   Netherlands |         1 |        0 |         1 |    2 |          2 |
 +---------------+-----------+----------+-----------+------+------------+

# pivoting without grouper factors

>>> cast_cons(q, [], ['born__country'], Count())
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+
 | England | Finland | Netherlands | New Zealand | Norway | South Africa | Sweden | Switzerland | USA | Count(all) |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+
 |       2 |       1 |           2 |           2 |      1 |            1 |      1 |           1 |   7 |         18 |
 +---------+---------+-------------+-------------+--------+--------------+--------+-------------+-----+------------+

# values are unwrapped (in this case some people had more than one occupation)

>>> cast_cons(q, ['occupation'], ['gender'])
 +--------------------------------+--------+------+------------+
 |                     occupation | female | male | Count(all) |
 +--------------------------------+--------+------+------------+
 |                 NetBSD founder |      0 |    0 |          1 |
 |                OpenBSD founder |      0 |    0 |          1 |
 |                OpenSSH founder |      0 |    0 |          1 |
 |           President of the FSF |      0 |    1 |          1 |
 |              Software engineer |      0 |    1 |          1 |
 |        author of Perl language |      0 |    1 |          1 |
 |      author of Python language |      0 |    1 |          1 |
 |           author of R language |      0 |    1 |          2 |
 |              computer engineer |      0 |    1 |          1 |
 |      computer language pioneer |      0 |    1 |          1 |
 |            computer programmer |      0 |    1 |          1 |
 |             computer scientist |      0 |    4 |          4 |
 |                   cryptanalyst |      0 |    1 |          1 |
 | invented mechanical calculator |      0 |    1 |          1 |
 |                       logician |      0 |    1 |          1 |
 |                  mathematician |      0 |    2 |          2 |
 |      original ENIAC programmer |      1 |    0 |          1 |
 |                     polititian |      0 |    1 |          1 |
 |                     programmer |      1 |    1 |          2 |
 +--------------------------------+--------+------+------------+

# all row cells are present regardless of data availability in grouper columns

>>> cast_cons(q, ['born__country', 'nick'], [], Count('nick'))
 +---------------+-------+-------------+
 | born__country |  nick | Count(nick) |
 +---------------+-------+-------------+
 |       England |  None |         N/A |
 |       Finland |  None |         N/A |
 |   Netherlands |  None |         N/A |
 |   New Zealand |  None |         N/A |
 |        Norway |  None |         N/A |
 |  South Africa |  None |         N/A |
 |        Sweden | Conny |           1 |
 |   Switzerland |  None |         N/A |
 |           USA |   Kay |           1 |
 |           USA |   Woz |           1 |
 |           USA |   rms |           1 |
 +---------------+-------+-------------+


# summary function

>>> summary(q, 'age')
 +-----+---------+--------+---------+---------+-----+
 | min | 1st qu. | median | average | 3rd qu. | max |
 +-----+---------+--------+---------+---------+-----+
 |  40 |    99.5 |   73.5 |    79.5 |    45.0 | 232 |
 +-----+---------+--------+---------+---------+-----+

# standard deviation function

>>> stdev(q, 'age')
42.176101401288442

# nested structures

>>> cast_cons(q, ['fullname__first'])
 +-----------------+------------+
 | fullname__first | Count(all) |
 +-----------------+------------+
 |            Alan |          1 |
 |           Anita |          1 |
 |          Donald |          1 |
 |           Guido |          1 |
 |           Linus |          1 |
 |         Richard |          1 |
 |         Stephen |          1 |
 |            Theo |          1 |
 +-----------------+------------+

>>> cast_cons(q, ['gender'], ['fullname__first'])
 +--------+------+-------+--------+-------+-------+---------+---------+------------+
 | gender | Alan | Anita | Donald | Guido | Linus | Richard | Stephen | Count(all) |
 +--------+------+-------+--------+-------+-------+---------+---------+------------+
 | female |    0 |     1 |      0 |     0 |     0 |       0 |       0 |          3 |
 |   male |    1 |     0 |      1 |     1 |     1 |       1 |       1 |         13 |
 +--------+------+-------+--------+-------+-------+---------+---------+------------+

"""
