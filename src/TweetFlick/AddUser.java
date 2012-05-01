package TweetFlick;

import Utilities.SettingsMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import org.json.JSONArray;
import org.json.JSONObject;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.PropertyConfiguration;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Karthik
 */
//ont time process of adding a celeb
public class AddUser {

    static Properties props = new Properties();
    static TwitterFactory factory = null;
    static Twitter twitter = null;


    static String[][] celebstr = {
 
{"soniya_agg","kollywood","tollywood","actress","female"}

    };


    public static void main(String[] args) {

//        getPage("https://api.twitter.com/1/statuses/user_timeline.json?page=1&screen_name=amuarora&include_rts=true&count=200");
//        if(1==1)System.exit(3);
for(int i=0;i<celebstr.length;i++){
    String[] celeb = celebstr[i];
    String name = celeb[0];
    ArrayList<String> tags = new ArrayList<String>();
    for(int j=1;j<celeb.length;j++){
        tags.add(celeb[j]);
    }
    addNewUser(name,tags);
}
if(2==2) System.exit(3);
        ArrayList<String> tags = new ArrayList<String>();
        tags.add("bollywood");
//        tags.add("kollywood");
//        tags.add("tollywood");
//        tags.add("hollywood");
//        tags.add("musician");
//        tags.add("singer");
        tags.add("actress");
        tags.add("female");
        addNewUser("KatrinaKaif_",tags);
    }

    static void initTwitter() {
        props.put("oauth.accessToken", SettingsMap.get("TWITTER_ACCESS_TOKEN"));
        props.put("oauth.accessTokenSecret", SettingsMap.get("TWITTER_ACCESS_TOKEN_SECRET"));
        props.put("oauth.consumerKey", SettingsMap.get("TWITTER_CONSUMER_KEY"));
        props.put("oauth.consumerSecret", SettingsMap.get("TWITTER_CONSUMER_SECRET"));
        connect();

    }

    static void connect() {
        factory = new TwitterFactory();
        Configuration conf = new PropertyConfiguration(props);

        AccessToken accessToken = new AccessToken(SettingsMap.get("TWITTER_ACCESS_TOKEN"), SettingsMap.get("TWITTER_ACCESS_TOKEN_SECRET"));
        Authorization auth = new OAuthAuthorization(conf);
        twitter = factory.getInstance();
    }

   
    static void addNewUser(String screenName, ArrayList<String> tags) {
        int page = 1;
        int results = 0;
        int totalResults = 0;
        boolean isAdded = false;
//        do {
            String link = "https://api.twitter.com/1/statuses/user_timeline.json?page=" + page + "&screen_name=" + screenName + "&include_rts=true&count=200";
            String content = getPage(link);
//            System.out.println("" + content);
//            if(2==2) System.exit(3);
            try {
                JSONObject json = new JSONObject(content);
                String req = json.getString("request");
                if (!req.isEmpty()) {
                    System.out.println("RateLimited, so cannot add a new user now, try after few mins:" + screenName);
                    return;
                }
            } catch (Exception e) {
//                System.out.println("json ex:content:"+content);
            }
            try {
                JSONArray arr = new JSONArray(content);
                int len = arr.length();
                results = len;

                BasicDBObject[] docs = new BasicDBObject[len];
                int index = 0;
                while (index < len) {
                    JSONObject tw = arr.getJSONObject(index);
                    long statusId = tw.optLong("id", 0);
                    String text = tw.optString("text", "");
                    String timeStr = tw.optString("created_at", "");
                    String geo = tw.optString("geo");
                    if(geo == null || geo.equals("null")) geo="";
                    else System.out.println("For "+screenName+" geo:"+geo);
                    String inReplyTo = tw.optString("in_reply_to_screen_name");
                    long inReplyToTweet = tw.optLong("in_reply_to_status_id");
                    long time = System.currentTimeMillis();
                    try {
                        SimpleDateFormat ft = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
                        time = ft.parse(timeStr).getTime();
                    } catch (Exception e) {
                    }
                    JSONObject user = tw.getJSONObject("user");
                    String username = user.optString("screen_name", "");
                    long userid = user.optLong("id", 0);

                    if (screenName.equalsIgnoreCase(username) && !isAdded) {
                        String userName = user.optString("name");
                        String userCreated = user.optString("created_at", "");
                        String profileImgUrl = user.optString("profile_image_url", "");
                        String description = user.optString("description");
                        long userCreatedTime = System.currentTimeMillis();
                        try {
                            SimpleDateFormat ft = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
                            userCreatedTime = ft.parse(userCreated).getTime();
                        } catch (Exception e) {
                        }

                        int followers = user.getInt("followers_count");
                        int friends = user.getInt("friends_count");
                        int statuses = user.getInt("statuses_count");
                        String location = user.getString("location");
                        boolean verified = user.getBoolean("verified");
                        DBActions.insertUser(userid,userName, screenName, profileImgUrl,description, userCreatedTime,followers,friends,statuses,location,verified,tags);
                        isAdded = true;
                    }

                    BasicDBObject doc = new BasicDBObject();

                    doc.put("_id", statusId);
                    doc.put("text", text);
                    doc.put("celeb_id", userid);
                    doc.put("time", time);
                     doc.put("geo",geo);
                    doc.put("reply_to",inReplyTo);
                     doc.put("reply_to_tweet", inReplyToTweet);
                    docs[index] = doc;
                    index++;
//                    System.out.println("***"+text+"\n");
                    String[] imageUrl = ProcessTweet.getImage(text);
                    if(!imageUrl[0].isEmpty()){
                        System.out.println("inserting:"+imageUrl[0]+" for "+screenName);
                        DBActions.insertPic(statusId, imageUrl, userid, screenName, username, time);
                    }
                }
                totalResults += results;
//                System.out.println("insert:"+docs.length);
                DBActions.insertTweets(docs);
            } catch (Exception e) {
                System.out.println("Ex in parsing JSON:" + e);
                System.out.println("content:"+content);
            }
            page++;
//        } while (results >= 200);
        System.out.println(screenName+" added, total tweets inserted:" + totalResults);
    }

    static String getPage(String link) {
        String content = "";
        try {
            URL url = new URL(link);
            InputStream is = url.openConnection().getInputStream();
            content = new Scanner(is).useDelimiter("\\A").next();

        } catch (Exception e) {
            System.out.println("ex:" + e);
        }

        return content;
    }

    

}
