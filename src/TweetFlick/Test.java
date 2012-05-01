/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package TweetFlick;

import Utilities.MongoUtils;
import Utilities.SettingsMap;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import org.bson.types.ObjectId;


/**
 *
 * @author Karthik
 */
public class Test {

    public static void main(String[] args) {
        getTags();
//          System.out.println(new java.util.Date().getTime());
//        testMongo();
//        keywordsTest();
//        movieFromTweetTest();
    }

 static void oid(){
     ObjectId id = new ObjectId("hi");
     System.out.println(""+id.toString());
 }

    static void testMongo() {
        try {
            Mongo mongo = new Mongo("50.16.191.161", 27017);
            DB db = mongo.getDB("Testing");
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
             DBCollection coll2 = db.getCollection(SettingsMap.get("DB_TWEETS_COLL"));
//            String i = null;
//            BasicDBObject doc = new BasicDBObject();
//            doc.put("_id",2);
//            doc.put("doc_created",MongoUtils.getCurrentTime());
//            doc.put("screenName", "k4rtk");
//            doc.put("followers","85");
//            BasicDBObject type = new BasicDBObject();
//            type.put("field","actor");
//            type.put("cat1","hollywood");
//            type.put("cat2","actor");
//            type.put("cat2","writeagain");
//            doc.put("type",type);
//            coll.insert(doc);

            System.out.println("now will print it maadi");
           DBCursor cur = coll.find();
        while(cur.hasNext()) {
            System.out.println(cur.next());
        }


        } catch (Exception e) {
            System.out.println("ex:" + e);
        }

    }

    static void doc_created() {
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        System.out.println("" + dateFormatGmt.format(new java.util.Date()));
    }

 static ArrayList<ArrayList<String>> getTags() {
        ArrayList<ArrayList<String>> allcelebTags = new ArrayList<ArrayList<String>>();
        Mongo mongo = null;
        try {
            mongo = new Mongo(SettingsMap.get("DB_LOCATION"), 27017);
            DB db = mongo.getDB(SettingsMap.get("DB_NAME"));
            DBCollection coll = db.getCollection(SettingsMap.get("DB_CELEBS_COLL"));
            DBCursor cur = coll.find().limit(5);

            while (cur.hasNext()) {
                DBObject celeb = cur.next();
                List<String>  tags = (List<String>) celeb.get("tags");
                for(String t:tags) System.out.println(""+t);
                System.out.println("***");
//                System.out.println(new java.util.Date()+":Get celebs"+celeb);
            }
        } catch (Exception e) {
            System.out.println("ex in getCelebs:" + e);
        } finally {
            mongo.close();
        }
        return allcelebTags;

    }
   
}
