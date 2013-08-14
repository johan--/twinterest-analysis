# Comparing prediction results with those declared interesting in Twinteresting
# experiment.

import csv, sqlite3

source_db = "data/databases/twinterest.db"
prediction_file = "Data/predictions.csv"


# Get the total number of timelines assessed by experimenters:
def getNumberOfQuestionsAnswered():
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT count(*) FROM (SELECT DISTINCT session_id, question FROM timeline)").fetchall()
    return int(result[0][0])

# Get list of timeline IDs (made up of string of format <session_id>/<question>):
def getTimelineIDs():
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM (SELECT DISTINCT session_id, question FROM timeline)").fetchall()
    ids = []
    for row in result:
        ids.append(str(row[0])+"/"+str(row[1]))
    return ids

# Get dict of all timelines of form: <timeline_id>:[<tweets>]
def getAllTimelines():
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline").fetchall()
    timelines = {}
    for row in result:
        id = str(row[0])+"/"+str(row[1])
        if id not in timelines:
            timelines[id] = []
        timelines[id].append(row)
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


# Get the timeline given by a timeline ID:
def getTimeline(id):
    session_id = (id.split("/"))[0]
    question = int((id.split("/"))[1])
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline WHERE session_id = '%s' AND question = %d" % (session_id, question)).fetchall()
    return result

# Get the interesting Tweets of a timeline given by the timeline's ID:
def getInteresting(id):
    session_id = (id.split("/"))[0]
    question = int((id.split("/"))[1])
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline WHERE session_id = '%s' AND question = %d AND selected = 1" % (session_id, question)).fetchall()
    return result   

# return scores from expected retweet values for  each tweet from experimental set:
def retrieve_tweet_scores():
    f = open("Data/predictions.csv", "rb")
    reader = csv.reader(f)
    global_scores = {}
    user_scores = {}
    for i, row in enumerate(reader):
        # row[0]=id,row[1]=user_id,row[2]=text,row[3]=count,row[4]=g_pred,row[5]=u_pred
        if i > 0:
            r_count = int(row[3])+0.0
            g = int(row[4])+0.0
            u = int(row[5])+0.0
            global_scores[int(row[0])] = r_count/g
            user_scores[int(row[0])] = r_count/u
    return (global_scores, user_scores)


def assignScores(timeline, scores):
    new_timeline = []
    dictrows = [dict(tweet) for tweet in timeline]
    for tweet in dictrows:
        thing = scores[int(tweet['tweet_id'])]

        tweet['score'] = scores[int(tweet['tweet_id'])]
    return dictrows

#
# MAIN CODE:
#

# Get data:
g_scores, u_scores = retrieve_tweet_scores()
timelines = getAllTimelines()

# remove tweets we don't have scores for:
for timeline in timelines:
    tweets_to_remove = []
    for tweet in timelines[timeline]:
        if int(tweet['tweet_id']) not in g_scores:
            tweets_to_remove.append(tweet)
    for tweet in tweets_to_remove:
        timelines[timeline].remove(tweet)
    
# Scoring system in use:
scores = u_scores



timeline_list = []
selected_dict = {}
selected_total = {}
for t in timelines:
    timeline = assignScores(timelines[t], scores)
    ordered = sorted(timeline, key=lambda x: x['score'])
    disparity = scores[ordered[-1]['tweet_id']] - scores[ordered[0]['tweet_id']]
    num_selected = 0
    for tweet in timeline:
        if int(tweet['selected']) == 1:
            num_selected += 1
    timeline_list.append({'selected':num_selected,'disparity':disparity})
    if num_selected not in selected_dict:
        selected_dict[num_selected] = 0.0
        selected_total[num_selected] = 0.0
    selected_dict[num_selected] += disparity
    selected_total[num_selected] += 1.0
for selected in selected_dict:
    print selected,selected_dict[selected] / selected_total[selected]


exit()
# 1) Ordered question stuff:
for lim in range (1, 21):
    print "%d" % (lim),
    total = 0
    total_counter = 0
    for t in timelines:
        timeline = assignScores(timelines[t], scores)
        ordered = sorted(timeline, key=lambda x: x['score'])

        disparity = scores[ordered[-1]['tweet_id']] - scores[ordered[0]['tweet_id']]
        
        found = False 
        min_timeline = 17
        limit = lim

        if len(ordered) > min_timeline: 
            total_counter += 1
            try:
                for i in range(0,limit):
                    if ordered[i]['selected'] == 1 and found == False:
                        total += 1
                        found = True
            except:
                continue
        
#    print total,"/",total_counter,"="
    print "%f" % ((total+0.0)/(total_counter+0.0))
