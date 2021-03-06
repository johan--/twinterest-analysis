# Script to write prediction data to a single .csv file (Data/predictions.csv)


import sqlite3 as s
import subprocess
import os
import re

test_db = "data/databases/testing.db"
user_arff = "data/user_arff/"
global_arff = "data/global_arff/"
user_models = "data/user_models/"
global_models = "data/global_models/"

def getTestingData():
    con = s.connect(test_db)
    con.row_factory = s.Row
    c = con.cursor()
    experimental = c.execute("select * from tweets").fetchall()
    return experimental

def getTestingDataAsDict():
    con = s.connect(test_db)
    con.row_factory = s.Row
    c = con.cursor()
    experimental = c.execute("select * from tweets").fetchall()
    d = {}
    for item in experimental:
        d[item['id']] = item
    return d

def getPredictionsFromGlobalCorpus(read = False):
    output = ""
    if read == True:
        print "Making global predictions..."
        process = subprocess.Popen(["java", "-Xmx5500M", "weka.classifiers.bayes.BayesNet", "-T", global_arff+"/global_test.arff", "-l", global_models+"/bayes.model", "-p", "0"], stdout=subprocess.PIPE)
        output = process.communicate()[0]
        outFile = open("data/prediction_outputs/global.txt", "w")
        outFile.write(output)
        outFile.close()
    if read == False:
        print "Retrieving global predictions from file..."
        inFile = open("data/prediction_outputs/global.txt", "r")
        output = inFile.read()
    
    print "Parsing global predictions..."
    lines = output.split("\n")
    predictions = []
    counter = 0
    for line in lines:
        info = line.split()
        if len(info) == 4:
            predictions.append((info[2].split(":"))[1])

    ids = []
    for tweet in testing:
        ids.append(tweet['id'])
    return (predictions, ids)

def getPredictionsFromEachUserCorpus(read = False):
    predDict = {}
    if read == True:
        print "Making predictions..."
        for subdir, dirs, files in os.walk(user_models):
            for file in files:
                if file.endswith(".model"):
                    userid = (file.split("."))[0]
                    process = subprocess.Popen(["java", "-Xmx1500M", "weka.classifiers.bayes.BayesNet", "-T", user_arff+userid+"-testing.arff", "-l", user_models+userid+".model", "-p", "0"], stdout=subprocess.PIPE)
                    output = process.communicate()[0]
                    outFile = open("Data/prediction_outputs/"+userid+".txt", "w")
                    outFile.write(output)
                    outFile.close()
                    predDict[userid] = output
    if read == False:
        print "Retrieving user predictions from files..."
        for subdir, dirs, files in os.walk("Data/prediction_outputs"):
            for file in files:
                if file.endswith(".txt") and "global" not in file:
                    userid = (file.split("."))[0]
                    inFile = open("Data/prediction_outputs/"+file, "r")
                    predDict[userid] = inFile.read()

    print "Parsing user predictions..."
    predListDict = {}
    for key in predDict:
        lines = predDict[key].split("\n")
        predictions = []
        for line in lines:
            info = line.split()
            if len(info) == 4:
                predictions.append((info[2].split(":"))[1])
        predListDict[int(key)] = predictions
   
    tweets_of_user = {}
    for tweet in testing:
        if tweet['user_id'] not in tweets_of_user:
            tweets_of_user[tweet['user_id']] = []
        tweets_of_user[tweet['user_id']].append(int(tweet['id']))

    
    print "Ordering user predictions (to same as in experimental database and globals)..."
    userOrder = []
    for row in testing:
        userOrder.append(row['user_id'])
    user_predictions = []
    idOrder = []
    userSet = set()
    for user in userOrder:
        tweets_of_this_user = tweets_of_user[int(user)]
        counter = 0
        if not user in userSet:
            if user in predListDict:
                if len(tweets_of_this_user) == len(predListDict[user]):
                    for item in predListDict[user]:
                        user_predictions.append(item)
                        idOrder.append(tweets_of_this_user[counter])
                        counter += 1
            else:
                print "fatal error: could not find this user:"
                print user
                exit()

            userSet.add(user)
    return (user_predictions,idOrder)

def buildAllUserModels(in_dir=user_arff, out_dir=user_models, redo = False):
    for subdir, dirs, files in os.walk(in_dir):
        for file in files: 
            if file.endswith("-training.arff"):
                user_id = (file.split("-"))[0]
                doThisUser = True
                if os.path.isfile(out_dir+"/"+user_id+".model"):
                   doThisUser = False
                   if redo == True:
                       doThisUser = True
                
                if doThisUser == True:
                   print "building model for",file,"..."
                   process = subprocess.Popen(["java", "-Xmx1500M", "weka.classifiers.bayes.BayesNet", "-t", in_dir+"/"+file, "-d", out_dir+"/"+user_id+".model"], stdout=subprocess.PIPE)
                   # neccessary to make python wait for this to end before continuing: 
                   output = process.communicate()[0] 

# MAIN:

# Get all the tweets from the experimental database:
testing = getTestingData()
testingDict = getTestingDataAsDict()

# Build models from ARFF files in user_arff/x-training.arff. 
# set redo=False if want to skip users that models already exist for
buildAllUserModels(redo = False)

# Following two calls return a list of predictions for tweets in experimental database.
# Set read to False to read predictions from file rather than from model (need to generate file
# from model first time though)
global_predictions, g_ids = getPredictionsFromGlobalCorpus(read = False) # predicted against global corpus
user_predictions, u_ids = getPredictionsFromEachUserCorpus(read = False) # predicted against that user's other tweets

print "finished generating/reading predictions"

print len(global_predictions),len(g_ids)
gdict_preds = {}
for i,pred in enumerate(global_predictions):
    gdict_preds[g_ids[i]] = int(pred.split("-")[1])
print len(user_predictions),len(u_ids)
udict_preds = {}
for i,pred in enumerate(user_predictions):
    udict_preds[u_ids[i]] = int(pred.split("-")[1])

predFile = open("Data/predictions2.csv", "w")
predFile.write("tweet_id,retweet_count,global_prediction,user_prediction\n")
for id in udict_preds:
    u_pred = udict_preds[id]
    g_pred = gdict_preds[id]
    expected = testingDict[id]['retweet_count']
    predFile.write(str(id)+","+str(expected)+","+str(g_pred)+","+str(u_pred)+"\n")
predFile.close()
exit()
# Create some dictionaries to hold the preditcion/real data.
# Each dict has key tweet_id and has the predicted/real outcome as the content for that tweet.
# e.g. reals[1233456] = 3-6 retweets
reals = {}
user_ids = {}
globalPredictions = {}
userPredictions = {}
print len(global_predictions)
print len(user_predictions)
for i in range(len(global_predictions)):
    try:
        reals[testing[i]['id']] = testing[i]['retweet_count']
        user_ids[testing[i]['id']] = testing[i]['user_id']
        globalPredictions[testing[i]['id']] = int((global_predictions[i].split("-"))[1])
        userPredictions[testing[i]['id']] = int((user_predictions[i].split("-"))[1])

    except Exception as e:
        print e
        print user_predictions[i]
        exit()

print "Writing CSV file..."
exit()
counter = 0
total_counter = 0
# Perform checks to compare predictions to real data and write to file
predFile = open("Data/predictions.csv", "w")
predFile.write("tweet_id,user,text,retweet_count,global_prediction,user_prediction\n")
asciSearch = re.compile(r'[^\x00-\x80]+').search # get the Regex search expression ready (only ASCI allowed)
for key in globalPredictions:
    total_counter += 1
    # flush out tweets with non-ASCII characters:
    if not asciSearch(testingDict[key]['text']):
        # flush out @-replies:
        if not testingDict[key]['text'].startswith("@"):
            if testingDict[key]['text'].startswith("RT @"):
                try:
                    param, value = testingDict[key]['text'].split(":",1)
                    text = value
                except:
                    print "ERROR",testingDict[key]['text']
            else:
                text = testingDict[key]['text']
            text = text.replace("\n","")
            text = text.replace(",","")
            predFile.write(str(key)+","+str(user_ids[key])+",\""+text.encode(encoding='UTF-8',errors='strict')+"\","+str(reals[key])+","+str(globalPredictions[key])+","+str(userPredictions[key])+"\n")
        counter += 1
    #print key
    #print "    ",globalPredictions[key]
    #print "    ",userPredictions[key]
    #print "    ",reals[key]
print counter,"/",total_counter,"tweets accepted.\nFinished."
predFile.close()
