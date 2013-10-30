import sqlite3, csv

source_db = "data/databases/twinterest.db"

def get_all_timelines():
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline").fetchall()
    timelines = {}
    for row in result:
        id = str(row['session_id'])+"/"+str(row['question'])
        if id not in timelines:
            timelines[id] = []
        timelines[id].append(row)
   
    # Remove timelines with no tweet selections:
    to_remove = []
    for timeline in timelines:
        selected = False
        for tweet in timelines[timeline]:
            if int(tweet['selected']) == 1:
                selected = True
        if selected == False:
            to_remove.append(timeline)
    for timeline in to_remove:
        del timelines[timeline]
    return timelines

def get_timelines_of_session(session):
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline WHERE session_id = '%s' AND question > 1" % (session)).fetchall()

    timelines = {}
    for row in result:
       id = str(row['session_id'])+"/"+str(row['question'])
       if id not in timelines:
            timelines[id] = []
       timelines[id].append(row) 
    return timelines

def retrieve_tweet_scores():
    f = open("Data/predictions2.csv", "rb")
    reader = csv.reader(f)
    global_scores = {}
    user_scores = {}
    for i, row in enumerate(reader):
        # row[0]=id,row[1]=user_id,row[2]=text,row[3]=count,row[4]=g_pred,row[5]=u_pred
        if i > 0:
            r_count = int(row[1])+0.0
            g = int(row[2])+0.0
            u = int(row[3])+0.0
            global_scores[int(row[0])] = r_count/g
            user_scores[int(row[0])] = r_count/u
    return (global_scores, user_scores)

g_scores, u_scores = retrieve_tweet_scores()

def assignScores(timeline, scores):
    new_timeline = []
    dictrows = [dict(tweet) for tweet in timeline]
    for tweet in dictrows:
        thing = scores[int(tweet['tweet_id'])]

        tweet['score'] = scores[int(tweet['tweet_id'])]
    return dictrows

def assignCounts(timeline):
    new_timeline = []
    dictrows = [dict(tweet) for tweet in timeline]
    for tweet in dictrows:
        #thing = scores[int(tweet['tweet_id'])]

        #tweet['score'] = scores[int(tweet['tweet_id'])]
        tweet['score'] = tweet['tweet_retweet_count']
    return dictrows   

def avg(l):
    total = 0.0
    for item in l:
        total += item
    return (total / len(l))

#sess_id = "91fe4424-bc43-44c9-86d4-2b6e5ee85376"
sess_id = "12697c2c-2063-45bc-8a72-aa126159cae1"
scores = u_scores
do_all = True
timelines = None
if not do_all:
    timelines = get_timelines_of_session(sess_id)
else:
    timelines = get_all_timelines()
min_scores = []
max_scores = []
min_counts = []
max_counts = []
# remove tweets we don't have scores for:
for timeline in timelines:
    tweets_to_remove = []
    for tweet in timelines[timeline]:
        if int(tweet['tweet_id']) not in g_scores:
            tweets_to_remove.append(tweet)
    for tweet in tweets_to_remove:
        timelines[timeline].remove(tweet)
my_num = 0.0
iterated = 0
for t in timelines:
    iterated += 1
    timeline = assignScores(timelines[t],scores)
    ordered = sorted(timeline, key=lambda x: x['score'])
    ordered.reverse()
    if not do_all:
        print iterated,
    t_count = 0
    max_s = 0
    min_s = 9999999
    for tweet in timeline:
        if tweet['selected'] == 1:
            if tweet['score'] >= max_s:
                max_s = tweet['score']
            if tweet['score'] <= min_s:
                min_s = tweet['score']
            if not do_all:
                print tweet['score'],
    if max_s != 0 or min_s != 9999999:
        max_scores.append(max_s)
        min_scores.append(min_s)
    if not do_all:
        print ""
print avg(max_scores)
print avg(min_scores)
print "----------------"
iterated = 0
for t in timelines:
    iterated += 1
    timeline = assignCounts(timelines[t])
    ordered = sorted(timeline, key=lambda x: x['score'])
    ordered.reverse()
    if not do_all:
        print iterated,
    t_count = 0
    max_s = 0
    min_s = 9999999
    for tweet in timeline:
        if tweet['selected'] == 1:
            if tweet['score'] >= max_s:
                max_s = tweet['score']
            if tweet['score'] <= min_s:
                min_s = tweet['score']
            if not do_all:
                print tweet['score'],
    if max_s != 0 or min_s != 9999999:
        max_counts.append(max_s)
        min_counts.append(min_s)
    if not do_all:
        print ""
print avg(max_counts)
print avg(min_counts)

   
