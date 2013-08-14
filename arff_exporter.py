# Script to read the database in Data/tweets.db and output an ARFF file
# containing many tweet and user features to be used by Weka.
#
# WRITES 'BIG' ARFF FILES (experimental/training to train global model) and 
# MANY SMALL USER ARFF FILES (to train individual user models)

import sqlite3
import feature_manager

user_arff = "data/user_arff/"
global_arff = "data/global_arff/"
test_db = "data/databases/testing.db"
train_db = "data/databases/training.db"

# Write the structure of the ARFF file for all tweets of a single user.
# Difference is lack of user features in file structure.
def writeUserStructure(cutoffs, file):
    # outcome line is dynamic depending on the number of bins the retweet_count is
    # split into:
    outcomeLine = "@attribute outcome {"
    for i in range(len(cutoffs)-1):
        if i == 0:
            # Uncomment for bin numbers (e.g. 0, 1, 2, ...):
            #outcomeLine = outcomeLine + (str)(i)
            # Uncomment for bin ranges (e.g. 0-100, 100-300, 300-2000, ...)
            outcomeLine = outcomeLine + (str)(cutoffs[i]) + "-"+ (str)(cutoffs[i+1])
        else:
            #outcomeLine = outcomeLine + "," + (str)(i)
            outcomeLine = outcomeLine + "," + (str)(cutoffs[i]) + "-"+ (str)(cutoffs[i+1])
    outcomeLine = outcomeLine + "}\n"
    
    entireLine = """@relation stuff\n
    @attribute mention {False,True}\n
    @attribute tweet_length real\n
    @attribute url {False,True}\n
    @attribute hashtag {False,True}\n
    @attribute smiley {False,True}\n
    @attribute unsmiley {False,True}\n
    @attribute exclamation {False,True}\n
    @attribute question {False,True}\n
    @attribute rt {False,True}\n
    @attribute reply {False,True}\n
    """+outcomeLine+"""
    \n@data\n"""  
    
    file.write(entireLine)
    return entireLine
    # Use this for numerical outcome: @attribute outcome numeric\n
    # Use this for e.g.nom outcome: @attribute outcome {low,mid-low,mid-high,high}\n

    
# Write the structure of the ARFF file for all tweets from all users
# Difference is inclusion of user features in file structure.
# Hard-coded to ensure compatibility with our global model.
def writeGlobalStructure(file):
    outcomeLine = "@attribute outcome {0-1,1-3,3-7,7-14,14-26,26-49,49-116,116-336,336-1618,1618-17249,17249-10000000}"
    
    entireLine = """@relation stuff
    @attribute mention {False,True}
    @attribute tweet_length real
    @attribute url {False,True}
    @attribute hashtag {False,True}
    @attribute smiley {False,True}
    @attribute unsmiley {False,True}
    @attribute exclamation {False,True}
    @attribute question {False,True}
    @attribute rt {False,True}
    @attribute reply {False,True}
    @attribute followers real
    @attribute friends real
    @attribute followers_maxFollowers real
    @attribute followers_minFollowers real
    @attribute followers_avgFollowers real
    @attribute followers_maxFriends real
    @attribute followers_minFriends real
    @attribute followers_avgFriends real
    @attribute verified {0,1}
    @attribute statuses_count real
    @attribute listed_count real
    @attribute favourites_count real
    @attribute followers_avgStatuses real
    @attribute friends_maxFollowers real
    @attribute friends_minFollowers real
    @attribute friends_avgFollowers real
    @attribute friends_maxFriends real
    @attribute friends_minFriends real
    @attribute friends_avgFriends real
    @attribute friends_avgStatuses real
    @attribute followers_proportionVerified real
    @attribute friends_proportionVerified real
    """+outcomeLine+"""
    @data\n"""
    file.write(entireLine)
    
    # Use this for numerical outcome: @attribute outcome numeric\n
    # Use this for e.g. nom outcome: @attribute outcome {low,mid-low,mid-high,high}\n
 
 
# Generate ARFF file for all tweets from a sngle user in database.
# Does NOT include user features (since these would be equal for each tweet)
def writeUserTraining(user, tweets, bins):
    file = open(user_arff+str(user)+"-training.arff", "w")
    # Get the quarters for the cutoffs in the retweet counts:
    LINEAR_BIN = 1
    DISTRIBUTED_BIN = 2
    manager = feature_manager.FeatureManager(tweets, DISTRIBUTED_BIN, bins)
    cutoffs = manager.getCutoffs()
    
    attributeString = writeUserStructure(cutoffs, file)
    for tweet in tweets:
        tweetFeatures = manager.getTweetFeatures(tweet)

        for key, value in tweetFeatures.iteritems():
            tweetFeatures[key] = str(value)
        # If using outcome as nom, use outcome as String category (e.g. high, low, etc.).
        # If using outcome as numeric, use outcome as Int retweet_count:
        file.write(tweetFeatures['mention'] +"," + tweetFeatures['length'] + "," + tweetFeatures['url'] +
        "," + tweetFeatures['hashtag'] + "," + tweetFeatures['smiley'] + "," +
        tweetFeatures['unsmiley'] + "," + tweetFeatures['exclamation'] + "," + tweetFeatures['question'] + ","
        + tweetFeatures['retweet'] + "," + tweetFeatures['reply'] + "," + tweetFeatures['outcome'] + "\n")
    file.close()
    return attributeString


def writeUserTesting(user, tweets, attributeString):
    file = open(user_arff+str(user)+"-testing.arff", "w")
    # Get the quarters for the cutoffs in the retweet counts:
    LINEAR_BIN = 1
    DISTRIBUTED_BIN = 2
    manager = feature_manager.FeatureManager(tweets, DISTRIBUTED_BIN, bins)
    
    cutoffs = manager.getCutoffs() 
    file.write(attributeString) 
    for tweet in tweets:
        tweetFeatures = manager.getTweetFeatures(tweet)

        for key, value in tweetFeatures.iteritems():
            tweetFeatures[key] = str(value)
        # If using outcome as nom, use outcome as String category (e.g. high, low, etc.).
        # If using outcome as numeric, use outcome as Int retweet_count:
        file.write(tweetFeatures['mention'] +"," + tweetFeatures['length'] + "," + tweetFeatures['url'] +
        "," + tweetFeatures['hashtag'] + "," + tweetFeatures['smiley'] + "," +
        tweetFeatures['unsmiley'] + "," + tweetFeatures['exclamation'] + "," + tweetFeatures['question'] + ","
        + tweetFeatures['retweet'] + "," + tweetFeatures['reply'] + ",? \n")
    file.close()  
    
     
def writeGlobalTesting(tweets,bins,filename):
    file = open(global_arff+filename+".arff", "w")
    # Get the quarters for the cutoffs in the retweet counts:
    LINEAR_BIN = 1
    DISTRIBUTED_BIN = 2
    manager = feature_manager.FeatureManager(tweets, DISTRIBUTED_BIN, bins)
    cutoffs = manager.getCutoffs()
   
    writeGlobalStructure(file) 
    for tweet in tweets:
        tweetFeatures = manager.getTweetFeatures(tweet)
        userFeatures = manager.getUserFeatures(tweet)
        for key, value in tweetFeatures.iteritems():
            tweetFeatures[key] = str(value)
        for key, value in userFeatures.iteritems():
            userFeatures[key] = str(value)
        # If using outcome as nom, use outcome as String category (e.g. high, low, etc.).
        # If using outcome as numeric, use outcome as Int retweet_count:
    
        file.write(tweetFeatures['mention'] +"," + tweetFeatures['length'] + "," + tweetFeatures['url'] +
        "," + tweetFeatures['hashtag'] + "," + tweetFeatures['smiley'] + "," +
        tweetFeatures['unsmiley'] + "," + tweetFeatures['exclamation'] + "," + tweetFeatures['question'] + ","
        + tweetFeatures['retweet'] + "," + tweetFeatures['reply'] + "," +
        userFeatures['followers'] + "," + userFeatures['friends'] + "," +
        userFeatures['followers_maxFollowers'] + "," + userFeatures['followers_minFollowers']  + "," +
        userFeatures['followers_avgFollowers'] + "," + userFeatures['followers_maxFriends'] + "," +
        userFeatures['followers_minFriends'] + "," + userFeatures['followers_avgFriends'] + "," +
        userFeatures['verified'] + "," + userFeatures['statuses_count'] + "," + userFeatures['listed_count'] + "," +
        userFeatures['favourites_count'] + "," + userFeatures['followers_avgStatuses'] + "," +
        userFeatures['friends_maxFollowers'] + "," + userFeatures['friends_minFollowers'] + "," +
        userFeatures['friends_avgFollowers'] + "," + userFeatures['friends_maxFriends'] + "," +
        userFeatures['friends_minFriends'] + "," + userFeatures['friends_avgFriends'] + "," +
        userFeatures['friends_avgStatuses'] + "," +
        userFeatures['followers_proportionVerified'] + "," + userFeatures['friends_proportionVerified'] + "," +
        "?\n")
    file.close()


def writeUserARFF(train_path=None, test_path=None, bins=30):
    con, c = connectToDB(train_path)
    con2, c2 = connectToDB(test_path)

    trainingDict = {}
    results = c.execute("select * from tweets").fetchall()
    for result in results:
        userToCheck = result['user_id']
        if userToCheck not in trainingDict:
            trainingDict[userToCheck] = []
        trainingDict[userToCheck].append(result)

    testingDict = {}
    results2 = c2.execute("select * from tweets").fetchall()
    for result in results2:
        userToCheck = result['user_id']
        if userToCheck not in testingDict:
            testingDict[userToCheck] = []
        testingDict[userToCheck].append(result)
    
    for userid in trainingDict:
        attributeString = writeUserTraining(userid, trainingDict[userid], bins) 
        writeUserTesting(userid, testingDict[userid], attributeString)

def writeGlobalARFF(test_path = None, bins=30):
    con, c = connectToDB(test_path)

    results = c.execute("SELECT * FROM tweets").fetchall()
    writeGlobalTesting(results,bins,"global_test")



def connectToDB(d):
    con = sqlite3.connect(d)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    return (con, c)

# MAIN:
bins = 30.0
writeUserARFF(train_path=train_db, test_path=test_db)
writeGlobalARFF(test_path=test_db)
