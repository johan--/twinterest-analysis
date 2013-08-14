# Script to fetch the necessary supplimentary  TRAINING information from Twitter.
#
# Uses the DB from twinterest to get a set of the users who's Tweets were
# evaluated by the people taking part and populates two databases:
# - One containing user info on each user (including friends/followers)
# - Another containing recent Tweets of each user
#
# This is so:
# 1) Models can be built for each evaluated user's Tweets
# 2) ARFF files can be generated for each user evaluated
#
# Namely, we need to collect (for each user evaluated):
# - 100 recent followers (all_info)
# - 100 recent friends (all_info)
# - recent Tweets (so that user-centric models can be built)
#
# After retrieval, each user is saved to a 'user' database containing all
# necessary info on each user evaluated.
#
# After this, the actual 'tweet' database can be constructed with all of
# the tweets evaluated and each tweet's author's information.

import tweepy
import os, sqlite3, random, datetime, time,json

# Define Twitter API authentication information
consumer_key = 'EA2W2r3phQFaj5Ry9Kmfw'
consumer_secret = 'zcXHw2lSlxJTcYzljqpYaPjhsNpGLr6DAJ1s5OM'
access_token = '360028618-AIaAELpy1qhFZDP4GQSnpNRCKsApSi7yqhTAJU6w'
access_token_secret = '5BtCKjFfLAtpXVhE4EdpuIlUTJBzuPTGnYCTjvPIfE'

# Define general variables
source_db = "data/databases/twinterest.db"
user_db = "data/databases/users.db" # to hold all user features (for user models & building ARFF)
tweet_db = "data/databases/tweets.db" # to hold Tweet features (for user models)

class Twitter:
    def __init__(self):
        self.authorize()        
        
    def authorize(self):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)

# Do not use this directly.
# Use a connectToX() method instead
def connectToDB(d):
    con = sqlite3.connect(d)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    return (con, c)

def connectToSource():
    return connectToDB(source_db)

def connectToTweets():
    con, c = connectToDB(tweet_db)
    c.execute('''CREATE TABLE IF NOT EXISTS tweet(
                user_id INTEGER,
                id INTEGER,
                text TEXT,
                retweet_count INTEGER)''')
    con.commit()
    return (con, c)


def connectToUsers():
    con, c = connectToDB(user_db)
    c.execute('''CREATE TABLE IF NOT EXISTS user(
                id INTEGER,
                screen_name TEST, 
                name TEXT, 
                followers_count INTEGER,
                friends_count INTEGER,
                verified TEXT,
                statuses_count INTEGER,
                listed_count INTEGER,
                favourites_count INTEGER,
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
                friends_proportionVerified DOUBLE)''')
    con.commit()
    return (con, c)

# Get users to be processed (even if they already have been)
def getToDoUsers():
    con, c = connectToSource()
    result = c.execute("SELECT user_id FROM timeline").fetchall()
    userset = set()
    for row in result:
        userset.add(row['user_id'])
    return list(userset)

# Get users who have already been processed
def getDoneUsers():
    con, c = connectToUsers()
    result = c.execute("select id from user").fetchall()
    userset = set()
    for row in result:
        userset.add(row['id'])
    return list(userset)
    
def getUsersWithNoTweets():
    con, c = connectToUsers()
    con2, c2 = connectToTweets()
    users = c.execute("select * from user").fetchall()
    user_set = set()
    for user in users:
        tweets = c2.execute("SELECT * FROM tweet WHERE user_id="+str(user['id'])).fetchone()
        if tweets == None:
            user_set.add(user['id'])
    return list(user_set)
       
def getRecentTweets(user, pages):
    tweets = []
    for i in range(1,pages+1):
        try:
            retrieved = twitter.user_timeline(user_id=user, count=200, page=i)
        except tweepy.error.TweepError as e:
            print e
            if not "Rate" in e.message:
                return -1
            else:
                raise e
        for tweet in retrieved:
            tweets.append(tweet)
    return tweets

# Get up to 100 followers of the user object. Returns a list of User objects:
def getSomeFollowers(userObject):
    follower_ids = twitter.followers_ids(user_id=userObject.id,count=100)
    followers = []
    if len(follower_ids) > 0:
        followers = twitter.lookup_users(user_ids=follower_ids)
    return followers 
	

# Get up to 100 friends of the user object.
def getSomeFriends(userObject):
    friend_ids = twitter.friends_ids(user_id=userObject.id,count=100)
    friends = []
    if len(friend_ids) > 0:
        friends = twitter.lookup_users(user_ids=friend_ids)
    return friends

# Get some information on the followers retrieved for the uer.
# Returns as a dictionary of information:
def getFollowerInfo(followers):
    info = {}
    if len(followers) == 0:	
        info['maxFollowers'] = -1
        info['maxFriends'] = -1
        info['minFollowers'] = -1
        info['minFriends'] = -1
        info['avgFollowers'] = -1
        info['avgFriends'] = -1
        info['avgStatuses'] = -1
        info['proportionVerified'] = -1
        return info
    # For min/max followers/friends:
    info['maxFollowers'] = 0
    info['maxFriends'] = 0
    info['minFollowers'] = 100000000
    info['minFriends'] = 1000000000
    # For avg followers/friends/statuses:
    totalFollowers = 0
    totalFriends = 0
    totalStatuses = 0
    totalVerified = 0.0
    for follower in followers:
        if follower.followers_count >= info['maxFollowers']:
            info['maxFollowers'] = follower.followers_count
        if follower.followers_count <= info['minFollowers']:
            info['minFollowers'] = follower.followers_count
        if follower.friends_count >= info['maxFriends']:
            info['maxFriends'] = follower.friends_count
        if follower.friends_count <= info['minFriends']:
            info['minFriends'] = follower.friends_count
        if follower.verified == True:
            totalVerified  = totalVerified + 1
        totalFollowers = totalFollowers + follower.followers_count
        totalFriends = totalFriends + follower.friends_count
        totalStatuses = totalStatuses + follower.statuses_count
    
    info['avgFollowers'] = totalFollowers / len(followers)
    info['avgFriends'] = totalFriends / len(followers)
    info['avgStatuses'] = totalStatuses / len(followers)
    info['proportionVerified'] = totalVerified / (0.0 + len(followers))
    return info

# Get some basic info on the user object. Returns as dictionary of information:
def getBasicInfo(userObject):
    info = {}
    if userObject.verified == True:
        info['verified'] = 1
    if userObject.verified == False:
        info['verified'] = 0	
    info['verified'] = userObject.verified
    info['listed_count'] = userObject.listed_count
    info['favourites_count'] = userObject.favourites_count
    info['statuses_count'] = userObject.statuses_count
    return info
	
# Write the relevant data to sqlite file (make table first if doesn't exist):
def storeUser(userObject, userInfo, followerInfo, friendInfo):
    con, c = connectToUsers()
    insertion = (
        userObject.id,userObject.screen_name,userObject.name,
        userObject.followers_count, userObject.friends_count,
        userInfo['verified'],userInfo['statuses_count'],
        userInfo['listed_count'],userInfo['favourites_count'],
        followerInfo['maxFollowers'],followerInfo['minFollowers'],
        followerInfo['avgFollowers'],followerInfo['maxFriends'],
        followerInfo['minFriends'],followerInfo['avgFriends'],
        followerInfo['avgStatuses'],followerInfo['proportionVerified'],
        friendInfo['maxFollowers'],friendInfo['minFollowers'],
        friendInfo['avgFollowers'],friendInfo['maxFriends'],
        friendInfo['minFriends'],friendInfo['avgFriends'],
        friendInfo['avgStatuses'],friendInfo['proportionVerified'])
    c.execute("""INSERT INTO user(
                id,screen_name,name,followers_count,
                friends_count,verified,statuses_count,listed_count,favourites_count,
                followers_maxFollowers,followers_minFollowers,followers_avgFollowers,
                followers_maxFriends,followers_minFriends,followers_avgFriends,
                followers_avgStatuses,followers_proportionVerified,
                friends_maxFollowers,friends_minFollowers,friends_avgFollowers,
                friends_maxFriends,friends_minFriends,friends_avgFriends,
                friends_avgStatuses,friends_proportionVerified)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", insertion)
    con.commit()

def storeTweets(user, tweets):
    con, c = connectToTweets()
    for tweet in tweets:            
        insertion = (user,tweet.id,tweet.text,tweet.retweet_count)
        c.execute("""INSERT INTO tweet(user_id, id, text, retweet_count) 
                VALUES(?,?,?,?)""", insertion)
    con.commit()


def waitOutLimit():
    print datetime.datetime.now().time()
    print "waiting 15 minutes..."
    time.sleep(15*60)
    print "resuming"

# MAIN CODE:
twitter = Twitter().api # authenticating as cardfiffmcserver

usersToDo = getToDoUsers()
usersDone = getDoneUsers()
print "Users remaining:",len(usersToDo) - len(usersDone)
tweetPages = 5 # 2 yields around 330 tweets; 4 yields around 650 tweets

for user in usersToDo:
    if user not in usersDone:
        try:
            # Get some basic info on the user:
            print "Starting User:",user
            userObject = twitter.get_user(user)
            userId = userObject.id
            print "Working...",
            # Get information on the user, its followers and friends, and then store:
            userInfo = getBasicInfo(userObject)
            someFollowers = getSomeFollowers(userObject)
            followerInfo = getFollowerInfo(someFollowers)
            someFriends = getSomeFriends(userObject)
            friendInfo = getFollowerInfo(someFriends)
            tweets = getRecentTweets(user, tweetPages)
            storeUser(userObject, userInfo, followerInfo, friendInfo)
            if tweets == -1:
                print "error processing user's tweets"
            else:
                print len(tweets),"Tweets collected"
                storeTweets(user, tweets)
            print "Done."
 
        except Exception as e:
            print "entire e:",e
            if "'Rate" in e.message or "Rate" in e.message:
                waitOutLimit()
            else:
                "Unsuccessful"
    else:
        print "Done user ",user,"(skipping...)",
