# Class to handle the feature generation for the training data.

class FeatureManager:    
    def __init__(self, tweets, binType, numBins):
        self.tweets = tweets
        self.binType = binType
        self.numBins = numBins
        self.LINEAR_BIN = 1
        self.DISTRIBUTED_BIN = 2
    
    def getCutoffs(self):
        if self.binType == self.LINEAR_BIN:
            self.cutoffs = self.getLinearCutoffs(self.tweets)
        if self.binType == self.DISTRIBUTED_BIN:
            self.cutoffs =  self.getDistributedCutoffs(self.tweets)
        return self.cutoffs
        
    def getOutcome(self, tweet):
        retweet_count = tweet['retweet_count']
        for i in range(0, len(self.cutoffs)-1):
            if retweet_count >= self.cutoffs[i] and retweet_count <= self.cutoffs[i+1]:
                outcome = str(self.cutoffs[i])+"-"+str(self.cutoffs[i+1])
        return outcome
        
    def getTweetFeatures(self, tweet):
        # tweet([]): 0=id,1=user_id,2=user_name,3=count,4=text,5=followers,6=friends,
        #7=followers_maxFollowers,8=followers_minFollowers,9=followers_avgFollowers,
        #10=followers_maxFriends,11=followers_minFriends,12=followers_avgFriends,
        #13=verified,14=statuses_count,15=listed_count,16=favourites_count,
        #17=followers_avgStatuses,18=friends_maxFollowers,19=friends_minFollowers,
        #20=friends_avgFollowers,21=friends_maxFriends,22=friends_minFriends,
        #23=friends_avgFriends,24=friends_avgStatuses,25=followers_proportionVerified,
        #26=friends_proportionVerified
        features = {}
        text = tweet['text']
        features['mention'] = self.hasMention(text)
        features['url'] = self.hasURL(text)
        features['hashtag'] = self.hasHashtag(text)
        features['question'] = self.isQuestion(text)
        features['exclamation'] = self.containsExclamation(text)
        features['length'] = self.getLength(text)
        features['reply'] = self.isReply(text)
        features['retweet'] = self.isRT(text)
        features['smiley'] = self.containsSmileys(text)
        features['unsmiley'] = self.containsUnSmileys(text)
        features['outcome'] = self.getOutcome(tweet)
        return features
    
    def getUserFeatures(self, tweet):
        features = {}
        features['followers'] = tweet['followers_count']
        features['friends'] = tweet['friends_count']
        features['followers_maxFollowers'] = tweet['followers_maxFollowers']
        features['followers_minFollowers'] = tweet['followers_minFollowers']
        features['followers_avgFollowers'] = tweet['followers_avgFollowers']
        features['followers_maxFriends'] = tweet['followers_maxFriends']
        features['followers_minFriends'] = tweet['followers_minFriends']
        features['followers_avgFriends'] = tweet['followers_avgFriends']
        features['verified'] = tweet['verified'] # value= 0/1 (instead of False/True)
        features['statuses_count'] = tweet['statuses_count']
        features['listed_count'] = tweet['listed_count']
        features['favourites_count'] = tweet['favourites_count']
        features['followers_avgStatuses'] = tweet['followers_avgStatuses']
        features['friends_maxFollowers'] = tweet['friends_maxFollowers']
        features['friends_minFollowers'] = tweet['friends_minFollowers']
        features['friends_avgFollowers'] = tweet['friends_avgFollowers']
        features['friends_maxFriends'] = tweet['friends_maxFriends']
        features['friends_minFriends'] = tweet['friends_minFriends']
        features['friends_avgFriends'] = tweet['friends_avgFriends']
        features['friends_avgStatuses'] = tweet['friends_avgStatuses']
        features['followers_proportionVerified'] = tweet['followers_proportionVerified']
        features['friends_proportionVerified'] = tweet['friends_proportionVerified']
        return features
            
    
    ######################
    # Tweet features methods
    #
    
    def hasMention(self, text):
        if "@" in text: return True
        else: return False
            
    def hasURL(self, text):
        if "http://" in text: return True
        else: return False
    
    def hasHashtag(self, text):
        if "#" in text: return True
        else: return False
    
    def isQuestion(self, text):
        if "?" in text: return True
        else: return False
            
    def containsExclamation(self, text):
        if "!" in text:	return True
        else: return False
    
    def getLength(self, text):
        return len(text)
    
    def isReply(self, text):
        if text.startswith("@"): return True
        else: return False
    
    def isRT(self, textIn):
        text = textIn.lower()
        if text.startswith("rt"): return True
        else: return False
    
    def containsSmileys(self, textIn):
        text = textIn.lower()
        smileyList = [":)",":-)",";)",";-)",":d",":-d","xd",":p",":-p"]
        for item in smileyList:
            if item in text:
                return True
        else: return False
    
    def containsUnSmileys(self, text):
        unSmileyList = [":(",":-(",";(",";-("]
        for item in unSmileyList:
            if item in text:
                return True
        else: return False
        
        
    ######################
    # Cutoff methods
    #
    
    def getLinearCutoffs(self, tweets):
        lowestRetweets = 1000000
        highestRetweets = 0
        
        for tweet in tweets:
            count = tweet['retweet_count']
            if count > highestRetweets:
                highestRetweets = count
            if count < lowestRetweets:
                lowestRetweets = count
        difference = highestRetweets - lowestRetweets
        intervalDifferences = difference/split
        print "lowest retweets:" + str(lowestRetweets)
        print "highest retweets:" + str(highestRetweets)
        print "interval from split:" + str(intervalDifferences)
        intervals = []
        for i in range (0,int(self.numBins)+1):
            intervals.append(lowestRetweets + (i*intervalDifferences))
        return intervals

    def getDistributedCutoffs(self, tweets):
        # First: extract retweet outcomes and sort:
        counts = []
        intervals = []
        total = 0
        for tweet in tweets:
            counts.append(tweet['retweet_count'])
            total = total + tweet['retweet_count']
        counts.sort()
        M = counts[-1] # highest retweet outcome in counts
        T = self.numBins # total number of bins
        N = len(counts) # total number of objects (retweet counts)
        TSum = N/T # rough number of objects for each bin
        s = 0
        
        #print "M=",M
        #print "T=",T
        #print "N=",N
        #print "Tsum=",TSum
        # Build histogram, H, as a dictionary storing number of times each
        # retweet outcome occurs
        H = {}
        for count in counts:
            if count in H:
                H[count] += 1
            else:
                H[count] = 1
        #print 'h',H 
        for i in range (0, M+1):
            if H.has_key(i):
                s += H[i]
            if s >= TSum:
                intervals.append(i)
                s = 0
        # Prepend a 0, if not already there:
        if 0 not in intervals:
            intervals.insert(0, 0)
        if intervals == [0]:
#            print "empty encountered! retweet count total:",total
            intervals = [0,1]
        # Append 100,000,000 (to act as upper bound):
        intervals.append(10000000)
#        print 'final intervals',intervals
        return intervals
        
    def getOldDistributedCutoffs(self, tweets):
        counts = []
        for tweet in tweets:
            counts.append(tweet['retweet_count'])
        
        counts.sort()
        print "lowest: ",counts[0]
        print "highest: ",counts[-1]
        intervalSize = int(len(counts)/(split-1))
        bins = []
        counter = 0
        currentBin = 0
        for count in counts:
            if (len(bins)-1) < currentBin:
                bins.append([])
            bins[currentBin].append(count)
            counter += 1
            if counter == intervalSize:
                counter = 0
                currentBin += 1
        intervals = []
        for i in range(0, len(bins)):
            if i == 0:
                intervals.append(bins[i][0])
            intervals.append(bins[i][-1])
        return intervals
