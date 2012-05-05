package TweetFlick;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import Utilities.SettingsMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.bson.types.ObjectId;

/**
 *
 * @author Karthik
 */
public class DBActions {

    static HashMap<String, ObjectId> keywordToId = new HashMap<String, ObjectId>();
    static MongoURI uri;
    static DB db;
    static {
    	 uri = new MongoURI(SettingsMap.get("DB_URI"));
	try {
		db = uri.connectDB();
		db.authenticate(uri.getUsername(), uri.getPassword());
	} catch (MongoException e) {
		e.printStackTrace();
	} catch (UnknownHostException e) {
		e.printStackTrace();
	}
	
    }
    public static void main(String[] args) {
//        insertUser(new Long("12121"), "gen", "", new Long("1212"), null);
    }

    //used in GetTweets
    static long[] getUserIds() {
        ArrayList<Object> useridObjs = new ArrayList<Object>();
        long[] userids = null;
        try {            
            

            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            BasicDBObject fieldsToretrieve = new BasicDBObject();
            fieldsToretrieve.put("_id", 1);
            DBCursor cur = coll.find(new BasicDBObject(), fieldsToretrieve);
            while (cur.hasNext()) {
                DBObject id = cur.next();
                useridObjs.add((Long) id.get("_id"));
            }
            userids = new long[useridObjs.size()];
            for (int i = 0; i < useridObjs.size(); i++) {
                userids[i] = (Long) useridObjs.get(i);
            }
        } catch (Exception e) {
            System.out.println("ex in getUserIds:" + e);
        } finally {
         //   mongo.close();
        }
        return userids;
    }

    static Map<String, Long> getCelebsForReply() {
        HashMap<String, Long> celebs = new HashMap<String, Long>();
        try {            
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            DBCursor cur = coll.find();

            while (cur.hasNext()) {
                DBObject celeb = cur.next();
                long id = (Long) celeb.get("_id");
                String screenName = (String) celeb.get("screenName");
                celebs.put(screenName.toLowerCase(), id);
//                System.out.println(new java.util.Date()+":getCelebsForReply::"+celeb);
            }
        } catch (Exception e) {
            System.out.println("ex in getCelebsForReply:" + e);
        } finally {
         //   mongo.close();
        }
        return celebs;
    }

    //used in persistTweets
    static Map<Long, DBObject> getCelebs() {
        HashMap<Long, DBObject> celebs = new HashMap<Long, DBObject>();
        try {            
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            DBCursor cur = coll.find();

            while (cur.hasNext()) {
                DBObject celeb = cur.next();
                long id = (Long) celeb.get("_id");
                celebs.put(id, celeb);
//                System.out.println(new java.util.Date()+":Get celebs"+celeb);
            }
        } catch (Exception e) {
            System.out.println("ex in getCelebs:" + e);
        } finally {
            //mongo.close();
        }
        return celebs;

    }
// used in persistTweets

    static void updateCelebs(Map<Long, DBObject> celebs) {
        try {            
            Map<Long, DBObject> celebsDb = getCelebs();
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            coll.rename(SettingsMap.get("DB_CELEBS_COLL_BCK"), true);
            DBCollection newColl = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            BasicDBObject indexes = new BasicDBObject();
            indexes.put("tags", 1);
            indexes.put("screenName", 1);
            newColl.ensureIndex(indexes);
            celebsDb.putAll(celebs);
            ArrayList<DBObject> celebsArr = new ArrayList<DBObject>();
            celebsArr.addAll(celebsDb.values());

            newColl.insert(celebsArr);
            System.out.println("updated celebs:" + celebsArr.size());

        } catch (Exception e) {

            System.out.println("ex in updateCelebs:" + e);
        } finally {
          //  mongo.close();
        }
    }

    static void insertTweet(long tweetId, String text, long celeb_id, long time,String geo, String inReplyTo,long inReplyToTweet) {
    	MongoURI uri;
        try {            
            DBCollection coll = null;
            BasicDBObject doc = new BasicDBObject();
            BasicDBObject indexes = new BasicDBObject();

            coll = db.getCollection(SettingsMap.get("DB_TWEETS_COLL"));
            doc.put("celeb_id", celeb_id);
//            ObjectId id = new ObjectId(tweetId + "");
            doc.put("_id", tweetId);
            doc.put("text", text);
            doc.put("time", time);
            doc.put("geo",geo);
            doc.put("reply_to", inReplyTo);
            doc.put("reply_to_tweet", inReplyToTweet);
            coll.insert(doc);

            indexes.put("celeb_id", 1);
            indexes.put("time", -1);
            indexes.put("geo",1);
            indexes.put("reply_to", 1);
            coll.ensureIndex(indexes);
//            System.out.println("InsertTweet:" + doc);
        } catch (Exception e) {
            System.out.println("ex in insertTweet:" + e);
        } finally {
            //mongo.close();
        }
    }

    static void insertFanTweet(long tweetId, String text, long fanId, String fanName, String fanScreenName, String profilePicUrl, long time, String inReplyTo,long inReplyToTweet) {
        try {    
            DBCollection coll = null;
            BasicDBObject doc = new BasicDBObject();
            BasicDBObject indexes = new BasicDBObject();
            coll = db.getCollection(SettingsMap.get("DB_FAN_TWEETS_COLL"));
            doc.put("_id", tweetId);
            doc.put("text", text);
            doc.put("time", time);
            doc.put("fan_id", fanId);
            doc.put("name", fanName);
            doc.put("screenName", fanScreenName);
            doc.put("profileImgUrl", profilePicUrl);
            doc.put("reply_to", inReplyTo);
            doc.put("reply_to_tweet", inReplyToTweet);
            coll.insert(doc);

            indexes.put("time", -1);
            indexes.put("reply_to", 1);
            coll.ensureIndex(indexes);
//            System.out.println("InsertTweet:" + doc);
        } catch (Exception e) {
            System.out.println("ex in insertFanTweet:" + e);
        } finally {
           // mongo.close();
        }
    }

    static void insertTweets(BasicDBObject[] docs) {
    	MongoURI uri;
        try {            
            DBCollection coll = db.getCollection(SettingsMap.get("DB_TWEETS_COLL"));

            coll.insert(docs);
            BasicDBObject indexes = new BasicDBObject();
            indexes.put("celeb_id", 1);
            indexes.put("time", -1);
            indexes.put("geo",1);
            indexes.put("reply_to", 1);
            coll.ensureIndex(indexes);
            System.out.println("Inserted tweets in batch:" + docs.length);
        } catch (Exception e) {
            System.out.println("ex in batch insert tweets:" + e);
        } finally {
          //  mongo.close();
        }
    }

    static void insertUser(long userId, String userName, String screenName, String profileImgUrl,String description, long created, int followers, int friends, int statuses, String location, boolean verified, ArrayList<String> tags) {
        try {            
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));

            int isVerified = (verified ? 1 : 0);


            BasicDBObject doc = new BasicDBObject();
            doc.put("_id", userId);
            doc.put("name", userName);
            doc.put("screenName", screenName);
            doc.put("profileImgUrl", profileImgUrl);
            doc.put("description",description);
            doc.put("created", created);
            doc.put("followers", followers);
            doc.put("friends", friends);
            doc.put("statuses", statuses);
            doc.put("isVerified", isVerified);
            doc.put("location", location);

            doc.put("tags", tags);
            coll.insert(doc);
            BasicDBObject indexes = new BasicDBObject();
            indexes.put("tags", 1);
            indexes.put("screenName", 1);
            coll.ensureIndex(indexes);
            System.out.println("inserted user:" + screenName);
        } catch (Exception e) {
            System.out.println("ex in insert user:" + e);
        } finally {
          //  mongo.close();
        }
    }
static void insertPic(long tweetId, String[] urls,long celeb_id, String screenName, String name,long time ) {
    try {            
            DBCollection coll = null;
            BasicDBObject doc = new BasicDBObject();
            BasicDBObject indexes = new BasicDBObject();

            coll = db.getCollection(SettingsMap.get("DB_PICS_COLL"));
            
//            ObjectId id = new ObjectId(tweetId + "");
            doc.put("_id", tweetId);
            doc.put("url", urls[0]);
            doc.put("small",urls[1]);
            doc.put("large",urls[2]);
            doc.put("celeb_id", celeb_id);
            doc.put("time",time );
            doc.put("celeb_id",celeb_id);
            doc.put("screenName",screenName);
            doc.put("name",name);
            coll.insert(doc);

            indexes.put("celeb_id", 1);
            indexes.put("time", -1);
            coll.ensureIndex(indexes);
//            System.out.println("InsertTweet:" + doc);
        } catch (Exception e) {
            System.out.println("ex in insertPic:" + e);
        } finally {
          //  mongo.close();
        }
    }

}
