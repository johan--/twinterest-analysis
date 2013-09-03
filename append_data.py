# Script to append the data from one database to another database.
#
# In particular, used to append the data from the meahcnical turk experiment to the
# current database. 

import sqlite3

original_db = "data/databases/twinterest.db"
new_db = "data/databases/twinterest_mkturk.db"

def connectToOriginal():
    con = sqlite3.connect(original_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    return (con, c)

def connectToNew():
    con = sqlite3.connect(new_db)
    con.row_factory = sqlite3.Row
    c = con.cursor()
    return (con, c)   

con1, new = connectToNew()
con2, orig = connectToOriginal()

stored_sessions = []
done_sessions = orig.execute("SELECT * FROM session").fetchall()
for s in done_sessions:
    stored_sessions.append(s['session_id'])

sessions = new.execute("SELECT * FROM session").fetchall()

for session in sessions:
    if session['session_id'] not in stored_sessions:
        if not int(session['question']) == 0:
            sid = session['session_id']
            mk_turk = session['mk_turk']
            
            # Append timeline table
            tweets = new.execute("SELECT * FROM timeline WHERE session_id = '"+sid+"'").fetchall()
            for t in tweets:
                insertion = (mk_turk,t['session_id'],t['question'],t['tweet_id'],t['tweet_text'],t['tweet_retweet_count'],t['user_id'],t['user_username'],t['user_profile_image'],t['user_name'],t['user_followers_count'],t['user_friends_count'],t['user_verified'],t['user_statuses_count'],t['user_favourites_count'],t['user_listed_count'],t['selected'])
                orig.execute("""INSERT INTO timeline(
                            mk_turk,session_id,question,tweet_id,tweet_text,tweet_retweet_count,user_id,user_username,user_profile_image,user_name,user_followers_count,user_friends_count,user_verified,user_statuses_count,user_favourites_count,user_listed_count,selected) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", insertion)

            # Append friend table   
            friends = new.execute("SELECT * FROM friend WHERE session_id = '"+sid+"'").fetchall()
            for friend in friends:
                l = []
                for item in friend:
                    l.append(item)
                orig.execute("INSERT INTO friend VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",l)

            # Append session table
            orig.execute("INSERT INTO session VALUES(?,?,?,?)" , (mk_turk, sid, session['timestamp'], int(session['question'])))

             # Append user table   
            users = new.execute("SELECT * FROM user WHERE session_id = '"+sid+"'").fetchall()
            for user in users:
                l = []
                for item in user:
                    l.append(item)
                orig.execute("INSERT INTO user VALUES(?,?,?,?,?,?,?,?,?,?,?)",l)       

con2.commit()


