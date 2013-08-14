# Script responsible for combining data from the 'users' and 'tweets' DBs 
# (created by 'user_getter.py') into a single experimental dataset.
#
# This new aggregated database is similar to ones used in ../Experimentation,
# and consist of a databse containing experimental data to be tested and training 
# data to generate the user-models.
#
# Thus, we create TWO databses:
# 1. Training database (contains Tweets collected in 'user_getter.py' and 
#   info on the author of each collected Tweet) to generate user-ARFFs from.
# 2. Experimental database (contains the Tweets which were evaluated during
#   Twinteresting experiment and info on each author of these Tweets).
#
# From 1., a series of user-ARFF files can be built to develop user models.
# From 2., the experimental data can be tested against the Tweets' authors'
# individual models and the global model used in previous experiments.

import sqlite3

source_db = "data/databases/twinterest.db"
tweet_db = "data/databases/tweets.db"
user_db  = "data/databases/users.db"
training_db = "data/databases/training.db"
testing_db = "data/databases/testing.db"

def connectToDB(d):
    con = sqlite3.connect(d)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    return (con, c)
def connectToSource():
    return connectToDB(source_db)
def connectToTweets():
    return connectToDB(tweet_db)
def connectToUsers():
    return connectToDB(user_db)
def connectToTraining():
    con, c = connectToDB(training_db)
    c.execute("""CREATE TABLE IF NOT EXISTS tweets(
                id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                retweet_count INTEGER,
                text TEXT,
                followers_count INTEGER,
                friends_count INTEGER,
                statuses_count INTEGER,
                listed_count INTEGER,
                favourites_count INTEGER,
                verified INTEGER,
                followers_maxFollowers INTEGER,
                followers_minFollowers INTEGER,
                followers_avgFollowers INTEGER,
                followers_maxFriends INTEGER,
                followers_minFriends INTEGER,
                followers_avgFriends INTEGER,
                followers_avgStatuses INTEGER,
                followers_proportionVerified DOUBLE,
                friends_maxFollowers INTEGER,
                friends_minFollowers INTEGER,
                friends_avgFollowers INTEGER,
                friends_maxFriends INTEGER,
                friends_minFriends INTEGER,
                friends_avgFriends INTEGER,
                friends_avgStatuses INTEGER,
                friends_proportionVerified DOUBLE)""")
    con.commit()
    return (con, c)

def connectToTesting():
    con, c = connectToDB(testing_db)
    c.execute("""CREATE TABLE IF NOT EXISTS tweets(
                selected INTEGER,
                id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                retweet_count INTEGER,
                text TEXT,
                followers_count INTEGER,
                friends_count INTEGER,
                statuses_count INTEGER,
                listed_count INTEGER,
                favourites_count INTEGER,
                verified INTEGER,
                followers_maxFollowers INTEGER,
                followers_minFollowers INTEGER,
                followers_avgFollowers INTEGER,
                followers_maxFriends INTEGER,
                followers_minFriends INTEGER,
                followers_avgFriends INTEGER,
                followers_avgStatuses INTEGER,
                followers_proportionVerified DOUBLE,
                friends_maxFollowers INTEGER,
                friends_minFollowers INTEGER,
                friends_avgFollowers INTEGER,
                friends_maxFriends INTEGER,
                friends_minFriends INTEGER,
                friends_avgFriends INTEGER,
                friends_avgStatuses INTEGER,
                friends_proportionVerified DOUBLE)""")
    con.commit()
    return (con, c)

def processTestingData():
    con, c = connectToSource()
    con2, c2 = connectToUsers()
    con3, c3 = connectToTesting()
    
    print "Generating test data database..."

    # fetch data:
    timeline = c.execute("SELECT * FROM timeline").fetchall()
    users = c2.execute("SELECT * FROM user").fetchall()

    # Build dict of users:
    user_dict = {}
    for user in users:
        user_dict[user['id']] = user
    
    null_counter = 0
    # Associate each tweet in each timeline evaluated with its 
    # author for which we have collected data using 'user_getter.py':
    for tweet in timeline:
        if tweet['user_id'] not in user_dict:
            print "[warning] could not find user",tweet['user_id']
            null_counter+= 1
        else:
            user = user_dict[tweet['user_id']]
            insertion=(tweet['selected'],tweet['tweet_id'],tweet['user_id'],tweet['user_username'],tweet['tweet_retweet_count'],tweet['tweet_text'],tweet['user_followers_count'],tweet['user_friends_count'],tweet['user_statuses_count'],tweet['user_listed_count'],tweet['user_favourites_count'],tweet['user_verified'],user['followers_maxFollowers'],user['followers_minFollowers'],user['followers_avgFollowers'],user['followers_maxFriends'],user['followers_minFriends'],user['followers_avgFriends'],user['followers_avgStatuses'],user['followers_proportionVerified'],user['friends_maxFollowers'],user['friends_minFollowers'],user['friends_avgFollowers'],user['friends_maxFriends'],user['friends_minFriends'],user['friends_avgFriends'],user['friends_avgStatuses'],user['friends_proportionVerified'])
            c3.execute("""INSERT INTO tweets(
                selected,id,user_id,user_name,retweet_count,text,
                followers_count,friends_count,statuses_count,listed_count,
                favourites_count,verified,
                followers_maxFollowers,followers_minFollowers,followers_avgFollowers,
                followers_maxFriends,followers_minFriends,followers_avgFriends,
                followers_avgStatuses,followers_proportionVerified,
                friends_maxFollowers,friends_minFollowers,friends_avgFollowers,
                friends_maxFriends,friends_minFriends,friends_avgFriends,
                friends_avgStatuses,friends_proportionVerified) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", insertion)
       
    print "[warning] unfound users:",null_counter
    print "Writing..." 
    con3.commit()
    con.close()
    con2.close()
    con3.close()
    print "Finished writing test data\n"

def processTrainingData():
    con, c = connectToTweets()
    con2, c2 = connectToUsers()
    con3, c3 = connectToTraining()
    
    print "Generating training data database..."

    # fetch data:
    tweets = c.execute("SELECT * FROM tweet").fetchall()
    users = c2.execute("SELECT * FROM user").fetchall()

    # Build dict of users:
    user_dict = {}
    for user in users:
        user_dict[user['id']] = user

    null_counter = 0
    for tweet in tweets:
        if tweet['user_id'] not in user_dict:
            null_counter += 1
            print "[warning] could not find user",tweet['user_id']
        else:
            user = user_dict[tweet['user_id']]
            insertion=(tweet['id'],tweet['user_id'],user['screen_name'],tweet['retweet_count'],tweet['text'],user['followers_count'],user['friends_count'],user['statuses_count'],user['listed_count'],user['favourites_count'],user['verified'],user['followers_maxFollowers'],user['followers_minFollowers'],user['followers_avgFollowers'],user['followers_maxFriends'],user['followers_minFriends'],user['followers_avgFriends'],user['followers_avgStatuses'],user['followers_proportionVerified'],user['friends_maxFollowers'],user['friends_minFollowers'],user['friends_avgFollowers'],user['friends_maxFriends'],user['friends_minFriends'],user['friends_avgFriends'],user['friends_avgStatuses'],user['friends_proportionVerified'])
            c3.execute("""INSERT INTO tweets(
                id,user_id,user_name,retweet_count,text,
                followers_count,friends_count,statuses_count,listed_count,
                favourites_count,verified,
                followers_maxFollowers,followers_minFollowers,followers_avgFollowers,
                followers_maxFriends,followers_minFriends,followers_avgFriends,
                followers_avgStatuses,followers_proportionVerified,
                friends_maxFollowers,friends_minFollowers,friends_avgFollowers,
                friends_maxFriends,friends_minFriends,friends_avgFriends,
                friends_avgStatuses,friends_proportionVerified) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", insertion)

    print "[warning] unfound users:",null_counter
    print "Writing..."
    con3.commit()

    con.close()
    con2.close()
    con3.close()
    print "Finished writing training data\n"


# MAIN CODE:

processTestingData()
processTrainingData()
print "Done."
