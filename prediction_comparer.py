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


# Get dict of all timelines of form: <timeline_id>:[<tweets>] that we have scores for:
def getAllTimelines(mk_turk = True, non_mk_turk = True):
    con = sqlite3.connect(source_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    result = c.execute("SELECT * FROM timeline").fetchall()
    timelines = {}
    for row in result:
        id = str(row['session_id'])+"/"+str(row['question'])
        if int(row['mk_turk']) == 1:
            if mk_turk == True:
                if id not in timelines:
                    timelines[id] = []
                timelines[id].append(row)
        if int(row['mk_turk']) == 0:
            if non_mk_turk == True:
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
timelines = getAllTimelines(mk_turk = False, non_mk_turk = True)

print len(timelines),"timelines to analyse"

# Scoring system in use:
scores = u_scores

# remove tweets we don't have scores for:
for timeline in timelines:
    tweets_to_remove = []
    for tweet in timelines[timeline]:
        if int(tweet['tweet_id']) not in g_scores:
            tweets_to_remove.append(tweet)
    for tweet in tweets_to_remove:
        timelines[timeline].remove(tweet)


# 10) chance of not picking top n non-interesting Tweets (polar opposite to exp1):
def exp10():
    totals = {}
    selected = {}
    for t in timelines:
        if len(timelines[t]) > 20:
            print t
            print "error"
            exit()
        timeline = assignScores(timelines[t], scores)
        ordered = sorted(timeline, key=lambda x: x['score'])

        # Only proceed with timelines having a precise number of Tweets selected:
        total_num_tweets = 0
        for tweet in ordered:
            if tweet['selected'] == 1:
                total_num_tweets += 1
        if total_num_tweets == 3:

            total_tweet_score = 0
            for tweet in ordered:
                total_tweet_score += tweet['score']

            if total_tweet_score >1:
                uninteresting = []
                for i in range(len(ordered)):
                    uninteresting.append(ordered[i])

                    if (i+1) not in totals:
                        totals[i+1] = 0.0
                        selected[i+1] = 0.0
                    totals[i+1] += 1.0
                    selected_check = False
                    for tweet in uninteresting:
                        if tweet['selected'] == 1:
                            selected_check = True
                    if selected_check == False:
                        selected[i+1] += 1.0
    for total in totals:
#        print selected[total]/totals[total] # proportion
        print selected[total] # raw no. timelines with this case


exp10()
exit()

# 9) heatmap of chosen Tweets
def exp9():
    heatmap = []
    total_for_avg = 0.0
    total_for_avg_total = 0.0
    total_proportion_avg = 0.0
    total_proportion_avg_total = 0.0
    for t in timelines:
        timeline = assignScores(timelines[t], scores)
        ordered = sorted(timeline, key=lambda x: x['score'])
        t_map = []
        t_map2 = []
        # replace with 'ordered' for heatmap of chosen tweets in timelines ordered
        # by score
        for i,tweet in enumerate(timeline):            
            if tweet['selected'] == 1:
                t_map.append(i)
                t_map2.append((i+0.0)/(len(timeline)+0.0))
                total_for_avg += (i+0.0)/(len(timeline)+0.0)
                total_for_avg_total += 1
        heatmap.append(t_map2)
    row = 1
    f = open("heatmap1.txt","w")
    for m in heatmap:
        print row,
        f.write(str(row)+" ")
        for t in m:
            f.write(str(t)+" ")
            print t,
        row += 1
        print ""
        f.write("\n")
    f.close()
    print (total_for_avg / total_for_avg_total)

# 8) average ratio of scores of chosen tweets : all tweets
def exp8():
    ratios = []
    for t in timelines:
        timeline = assignScores(timelines[t],scores)
        ordered = sorted(timeline, key=lambda x: x['score'])
        total = []
        chosen = []
        num_selected = 0
        for tweet in timeline:
            total.append(tweet['score'])
            if int(tweet['selected'] == 1):
                num_selected += 1
                chosen.append(tweet['score'])
        if num_selected > 0:
            total_chosen = 0.0
            total_all = 0.0
            for score in chosen:
                total_chosen += score
            avg_chosen = total_chosen / (len(chosen)+0.0)
            for score in total:
                total_all += score
            avg_all = total_all / (len(total)+0.0)
            ratio = total_chosen / avg_all
            ratios.append(ratio)
    total_ratio = 0.0
    for ratio in ratios:
        total_ratio += ratio
    print (total_ratio / len(ratios))


# 7) compare average score of selected tweets vs all tweets
def exp7():
    total_scores = []
    chosen_scores = []
    for t in timelines:
        timeline = assignScores(timelines[t],scores)
        ordered = sorted(timeline, key=lambda x: x['score'])
        total = []
        chosen = []
        num_selected = 0
        for tweet in timeline:
            total.append(tweet['score'])
            if int(tweet['selected'] == 1):
                num_selected += 1
                chosen.append(tweet['score'])
        if num_selected > 0:
            total_chosen = 0.0
            total_all = 0.0
            for score in chosen:
                total_chosen += score
            avg_chosen = total_chosen / (len(chosen)+0.0)
            for score in total:
                total_all += score
            avg_all = total_all / (len(total)+0.0)
            total_scores.append(avg_all)
            chosen_scores.append(avg_chosen)
    total_total = 0.0
    total_chosen = 0.0

    for avg in total_scores:
        total_total += avg
    for avg in chosen_scores:
        total_chosen += avg
    print (total_total / len(total_scores))
    print (total_chosen / len(chosen_scores))
 

# 6) compare disparity of selected tweets vs total disparity of timeline
def exp6():
    total_disparities = []
    chosen_disparities = []
    for t in timelines:
        timeline = assignScores(timelines[t],scores)
        ordered = sorted(timeline, key=lambda x: x['score'])
        total_disparity = scores[ordered[-1]['tweet_id']] - scores[ordered[0]['tweet_id']]
        chosen_scores = []
        num_selected = 0
        for tweet in timeline:
            if int(tweet['selected'] == 1):
                num_selected += 1
                chosen_scores.append(tweet['score'])
        ordered_chosen = sorted(chosen_scores)
        chosen_disparity = 0    
        if num_selected > 1:
            chosen_disparity = ordered_chosen[-1] - ordered_chosen[0]
        if num_selected > 0:
            total_disparities.append(total_disparity)
            chosen_disparities.append(chosen_disparity)
    total_total = 0.0
    total_chosen = 0.0
    for disparity in total_disparities:
        total_total += disparity
    for disparity in chosen_disparities:
        total_chosen += disparity
    print (total_total / len(total_disparities))
    print (total_chosen / len(chosen_disparities))

# 5) inverse of exp1 (i.e. how often are non-interesting Tweets NOT selected?)
def exp5():
     for lim in range (1, 21):
        print "%d" % (lim),
        total = 0
        total_counter = 0
        for t in timelines:
            timeline = assignScores(timelines[t], scores)
            ordered = sorted(timeline, key=lambda x: x['score'])
            ordered.reverse()
            if len(ordered) > 5:
                print ordered[0]['score']
                print ordered[-1]['score']
                disparity = scores[ordered[-1]['tweet_id']] - scores[ordered[0]['tweet_id']]
                
                found = False 
                min_timeline = 0
                limit = lim

                if len(ordered) > min_timeline: 
                    total_counter += 1
                    try:
                        for i in range(0,limit):
                            if ordered[i]['selected'] == 0 and found == False:
                                total += 1
                                found = True
                    except:
                        continue
            
        print "%f" % ((total+0.0)/(total_counter+0.0))


# 4) frequency distribution of number of selected tweets in each timeline
def exp4():
    selected_dict = {}
    for t in timelines:
        timeline = timelines[t]
        num_selected = 0
        for tweet in timeline:
            if int(tweet['selected']) == 1:
                num_selected += 1
        if num_selected not in selected_dict:
            selected_dict[num_selected] = 0
        selected_dict[num_selected] += 1
    for selected in selected_dict:
        print selected,selected_dict[selected]

# 3) general matching of selected vs. high-scored
def exp3():
    for thresh in range(1, 50):
        int_thresh = thresh
        num_interesting = 0.0
        num_interesting_and_scored = 0.0
        num_scored = 0.0
        num_scored_and_interesting = 0.0
        for t in timelines:
            timeline = assignScores(timelines[t], scores)
            for tweet in timeline:
                if int(tweet['selected']) == 1:
                    num_interesting += 1.0
                    if tweet['score'] > int_thresh:
                        num_interesting_and_scored += 1.0
                if tweet['score'] > int_thresh:
                    num_scored += 1
                    if int(tweet['selected']) == 1:
                        num_scored_and_interesting += 1
#    print thresh,(num_interesting_and_scored/num_interesting)
        print (num_scored_and_interesting/num_scored)
        #print "of selected, we agree:",(num_interesting_and_scored/num_interesting)
        #print "of scored high, THEY agree:",(num_scored_and_interesting/num_scored)


# 2) Num selections vs disparity
def exp2():
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



# 1) Ordered question stuff:
def exp1():
    total_perc_perc = {}
    total_perc_total = {}

    total_lengths = 0
    for t in timelines:
        total_lengths += len(timelines[t])

    for lim in range (1, 21):
    #    print "%d" % (lim),
        total = 0
        total_counter = 0
        for t in timelines:
            timeline = assignScores(timelines[t], scores)
            
            # Get number of selected tweets for this timeline:
            num_total_selected = 0
            for tweet in timeline:
                if tweet['selected'] == 1:
                    num_total_selected += 1

            # Get total sum of scores for timeline:
            total_score_sum = 0
            for tweet in timeline:
                total_score_sum += tweet['score']
             
            # Only proceed for timelines with a specific number of selections and if
            # the sum of all scores is greater than 1 (prevent cases with loads of 0s)
            if num_total_selected == 3 and total_score_sum > 0:

                ordered = sorted(timeline, key=lambda x: x['score'])
                ordered.reverse() #Ensure HIGHEST score is first
                found = False
                
                # NEED TO THINK ABOUT THIS STUFF (i.e. not all timelines
                # are of this length)
                min_timeline = 0
                limit = lim

                # Now see if a selected Tweet appears in top 'lim' of
                # Tweets for this timeline (if so, increment 'total'):
                if len(ordered) > min_timeline: 
                    total_counter += 1
                    try:
                        for i in range(0,limit):
                            if ordered[i]['selected'] == 1 and found == False:
                                total += 1
                                found = True
                    except:
                        continue
            
        print "%f" % ((total+0.0)/(total_counter+0.0)) # proportion of timelines
#        print total # This is the raw number of timelines with this case

exp1()

