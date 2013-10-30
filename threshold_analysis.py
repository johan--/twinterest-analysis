import csv, sqlite3

source_db = "data/databases/twinterest.db"
prediction_file = "Data/predictions.csv"

# Get dict of all timelines of form: <timeline_id>:[<tweets>] that we have scores for:
def getAllTimelines(mk_turk = True, non_mk_turk = True):
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
scores = None

def assignScores(timeline, scores):
    new_timeline = []
    dictrows = [dict(tweet) for tweet in timeline]
    for tweet in dictrows:
        thing = scores[int(tweet['tweet_id'])]

        tweet['score'] = scores[int(tweet['tweet_id'])]
    return dictrows

def assignCounts(timeline):
    dictrows = [dict(tweet) for tweet in timeline]
    for tweet in dictrows:
        tweet['score'] = tweet['tweet_retweet_count']
    return dictrows

def calculate_threshold(timeline, level, counts = False):
    if counts:
        timeline = assignCounts(timeline)
    if not counts:
        timeline = assignScores(timeline, scores)
    timeline = sorted(timeline, key=lambda x: x['score'])
    timeline.reverse()
    empty_counter = 0
    selected_counter = 0
    counter = 0
    for tweet in timeline:
        counter += 1
        if tweet['selected'] == 1:
            selected_counter += 1
        else:
            empty_counter += 1
        if empty_counter >= level:
            break
    if selected_counter != 0:
        return counter
    return -1

scores = u_scores
timelines = getAllTimelines()

# remove tweets we don't have scores for:
for timeline in timelines:
    tweets_to_remove = []
    for tweet in timelines[timeline]:
        if int(tweet['tweet_id']) not in g_scores:
            tweets_to_remove.append(tweet)
    for tweet in tweets_to_remove:
        timelines[timeline].remove(tweet)

totals = 0.0
for timeline in timelines:
    t = calculate_threshold(timelines[timeline], 4, counts=True)
    if t >= 0:
        totals+=t
print totals / (len(timelines)+0.0)
