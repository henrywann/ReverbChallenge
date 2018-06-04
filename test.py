import run_analysis as ra


num_of_passed = 0
'''
Test 1: Testing for empty txt file input
'''
data = 'test1.txt'
datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
top = 5
labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']

df = ra.populate_df(data, labels, datetime_format)
median, mean, Min, Max = ra.get_stats(df=df)
if df.empty and median == 0 and mean == 0 and Min == 0 and Max == 0:
    num_of_passed += 1

'''
Test 2: Testing for fewer lines of input than top n
'''
data = 'test2.txt'
datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
top = 5
labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']
df = ra.populate_df(data, labels, datetime_format)
top_visited_url = ra.get_top_url(df, top)
if top_visited_url == ['/orchestra', '/marketplace', '/pedals'] or top_visited_url == ['/orchestra', '/pedals', '/marketplace']:
    num_of_passed += 1

'''
Test 3: Testing for not enough data points
'''
data = 'test3.txt'
datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
top = 5
labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']
df = ra.populate_df(data, labels, datetime_format)
median, mean, Min, Max = ra.get_stats(df=df)
if median == -1 and mean == -1 and Min == -1 and Max == -1:
    num_of_passed += 1

'''
Test 4: Testing for most visited functionality
'''
data = 'test4.txt'
datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
top = 5
labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']
df = ra.populate_df(data, labels, datetime_format)
top_visited_url = ra.get_top_url(df, top)
if top_visited_url == ['/pedals', '/electric-guitars', '/orchestra', '/marketplace']:
    num_of_passed += 1

'''
Test 5: Testing for duration stats functionality
'''
data = 'test5.txt'
datetime_format = '%Y-%m-%dT%H:%M:%S.%f'
top = 5
labels = ['url', 'timestamp', 'sessionId', 'experiments', 'userId']
df = ra.populate_df(data, labels, datetime_format)
top_visited_url = ra.get_top_url(df, top)
median, mean, Min, Max = ra.get_stats(df=df)
if median == 27000.0 and mean == 27000.0 and Min == 27000.0 and Max == 27000.0:
    num_of_passed += 1

print("{} out of 5 tests passed!".format(num_of_passed))
