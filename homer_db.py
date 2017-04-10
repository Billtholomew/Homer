import ast
import datetime
from dateutil.parser import parse as parse_date
import matplotlib.pyplot as plt
from collections import defaultdict
from numpy import mean, ceil


def load_data(file_name, db, drop_all=False):
    if drop_all:
        for coll in db.collection_names():
            db[coll].drop()
    with open(file_name, 'r+') as file_in:
        for event in file_in:
            # read in string as dictionary, then load to mongo
            event = ast.literal_eval(event)                       
            # clean age data
            if 'age' in event.keys():
                age = event['age']
                try:
                    age = float(age)
                except: # We don't care why we can't get age, but if we can't make it None/null
                    age = None
                event['age'] = age
            # clean _t as well
            if '_t' in event.keys():
                _t = event['_t']
                try:
                    _t = datetime.datetime.fromtimestamp(float(_t))
                except:
                    _t = None
                event['_t'] = _t
            db['events'].insert_one(event)
    # create users collection
    pipeline = [
        {'$match': {'_n': {'$exists': False}, 'producttype': {'$exists': False}, 'name': {'$exists': False}}},
        {'$group': {'_id': {'_p': '$_p', 'name': '$name'},
                    '_p': {'$last': '$_p'}, 'name': {'$last': '$name'},
                    'age': {'$last': '$age'}, '_t': {'$last': '$_t'}}},
        {'$project': {'_id': 0, '_p': 1, 'name': 1, 'age': 1, '_t': 1}},
        {'$out': 'users'}
    ]
    db['events'].aggregate(pipeline)
    # create lessons collection
    pipeline = [
        {'$match': {'unitcategory': {'$in': ['DTW', 'LTR']}}},
        {'$group': {'_id': {'_p': '$_p', 'unitcategory': '$unitcategory', 'manuscripttitle': 'manuscripttitle'},
                    '_p': {'$last': '$_p'},
                    'unitcategory': {'$last': '$unitcategory'},
                    'manuscripttitle': {'$last': 'manuscripttitle'},
                    'session_events': {'$push': {'_n': '$_n', '_t': '$_t'}}
                    }},
        {'$project': {'_id': 0, '_p': 1, 'unitcategory': 1, 'manuscripttitle': 1, 'session_events': 1}},
        {'$out': 'lessons'}
    ]
    db['events'].aggregate(pipeline)
    print 'Load Data Complete'


def get_lessons_with_sessions(db, _p):
    session_events = db['lessons'].find({'_p': _p})
    return session_events


def generate_session_times(db, _p):
    lessons = get_lessons_with_sessions(db, _p)
    for lesson in lessons:
        lesson = lesson['session_events']
        lesson = sorted(lesson, key=lambda l: l['_t'])
        # there's an issue where the lesson can be closed without an event notification
        # this results in, for example, two opens in a row, keep only the latest
        n_events = len(lesson)
        lesson = [lesson[i] for i in xrange(n_events)
                  if (i + 1) == n_events or lesson[i]['_n'] != lesson[i + 1]['_n']]
        event_times = map(lambda event: event['_t'], lesson)
        # if we have an odd number of events, we probably opened without closing yet
        # drop that last open so it won't mess up calculations
        if len(event_times) % 2 == 1:
            event_times = event_times[:-1]
        event_times = map(lambda t: t, event_times)
        time_deltas = map(lambda (start, end): end - start, zip(event_times, event_times[1:]))
        for date, td in zip(event_times[0::2], time_deltas):
            yield (date, td.total_seconds())


def get_average_times(db, _p, count_outliers = False):
    months = defaultdict(int)
    weeks = defaultdict(int)
    days = []
    outliers = [0, 0]
    for date, total_seconds in generate_session_times(db, _p):
        if total_seconds > (1 * 3600):
            outliers[1] += 1
            continue
        if total_seconds < (1 * 60):
            outliers[0] += 1
            continue
        m = date.month
        w = date.isocalendar()[1]
        months[m] += total_seconds
        weeks[w] += total_seconds
        days.append(total_seconds)

    monthly_average = mean(months.values())
    weekly_average = mean(weeks.values())
    daily_average = mean(days)
    # convert time to minutes, rounding up
    times = {'daily': ceil(daily_average / 60),
            'weekly': ceil(weekly_average / 60),
            'monthly': ceil(monthly_average / 60)
            }
    if count_outliers:
        times['outliers'] = outliers
    return times

    
def get_age_histogram(db, collection):
    pipeline = [
        {'$match': {'age': {'$gte': 0, '$lte': 100}, 'name': {'$exists': True}}},
        {'$group': {'_id': '$age', 'count': {'$sum': 1}}},  # get how many of each age there are
        {'$sort': {'age': 1}}
    ]
    age_counts = db[collection].aggregate(pipeline)
    ages, counts = zip(*[(age_count['_id'], age_count['count']) for age_count in age_counts])
    plt.bar(ages, counts)
    plt.title('Age Distribution')
    plt.xlabel('Age')
    plt.ylabel('Count')
    plt.show()


# gets a distribution for ALL users of lesson lengths
# used for analysis
def get_lesson_histogram(db):
    from random import random
    unique_users = [user['_p'] for user in db['users'].find({'age': {'$gte': 1, '$lte': 10}})]
    session_times = defaultdict(int)
    num_users = len(unique_users)
    print num_users, '>', num_users * 0.025
    for i, _p in enumerate(unique_users):
        if random() > 0.025:
            continue
        for _, st in generate_session_times(db, _p):
            st = st / 60 * 60
            session_times[st] += 1
        print (i / float(num_users)) * 100
    return session_times


def get_valid_ages(db):
    pipeline = [
        {'$match': {'age': {'$gte': 0, '$lte': 100}}},
        {'$group': {'_id': '$age', 'count': {'$sum': 1}}},  # get how many of each age there are
        {'$sort': {'age': 1}}
    ]
    valid_ages = db['events'].aggregate(pipeline)

def get_lessons_with_ages(db):
    pipeline = [
        {'$lookup': {'from': 'lessons', 'localField': '_p', 'foreignField': '_p', 'as': 'lessondata'}},
        {'$project': {'age': 1, '_p': 1, 'lessondata.manuscripttitle': 1, 'lessondata.unitcategory': 1}},
        {'$unwind': '$lessondata'},
        {'$group': {'_id': '$age',
                    'manuscripts': {'$addToSet': '$lessondata.manuscripttitle'},
                    'categories': {'$addToSet': '$lessondata.unitcategory'}}}
    ]