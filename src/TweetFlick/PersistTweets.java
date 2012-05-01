package TweetFlick;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Message;
import Utilities.MQUtilities;
import Utilities.SettingsMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Map;

/**
 *
 * @author Karthik
 */
public class PersistTweets {

    javax.jms.Connection connection = null;
    Session session = null;
    javax.jms.Queue queue = null;
    MessageConsumer receiver = null;
    MQUtilities mqUtils = new MQUtilities();
    static Map<Long, DBObject> idToCelebMap;
    Map<String, Long> celebMap;
    static long userRefreshTime;
    static final long REFRESH_INTERVAL = 15 * 60 * 1000;

    static int tweetsCount=0;
    static int fanTweetsCount=0;

    public static void main(String[] args) {
        PersistTweets dequer = new PersistTweets();
        dequer.init();
        dequer.deque();
    }

    void init() {
        try {
            if (connection != null) {
                try {
                    session.close();
                } catch (JMSException je) {
                }
                try {
                    connection.close();
                } catch (Exception ex) {
                }
            }
            connection = mqUtils.getConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue("TWEETS_QUEUE");
            connection.start();
            receiver = session.createConsumer(queue);
            idToCelebMap = DBActions.getCelebs();
            celebMap = DBActions.getCelebsForReply();
            System.out.println("Init:total celebs found:" + idToCelebMap.size());
            userRefreshTime = System.currentTimeMillis();
        } catch (Exception e) {
            System.out.println(new java.util.Date() + ":Ex in init of TweetDeque:" + e);
        }
    }

    void deque() {
        while (true) {
            try {
                Message msg = receiver.receive();
                if (!(msg instanceof MapMessage)) {
                    continue;
                }
                MapMessage mapMsg = (MapMessage) msg;
                String text = mapMsg.getString("text");
                long tweetId = mapMsg.getLong("tweetId");
                long time = mapMsg.getLong("time");

                String userScreenname = mapMsg.getString("userScreenname");
                String userName = mapMsg.getString("userName");
                long userId = mapMsg.getLong("userId");
                String inReplyTo = mapMsg.getString("inReplyTo");
                long inReplyToTweet = mapMsg.getLong("inReplyToTweet");
                String geo = mapMsg.getString("geo");
                String userImage = mapMsg.getString("userImgurl");
                String description = mapMsg.getString("description");
                String location = mapMsg.getString("userLocation");
                boolean isVerifiedbool = mapMsg.getBoolean("isVerified");
                int isVerified = (isVerifiedbool ? 1 : 0);
                int followCount = mapMsg.getInt("userFollowCount");
                int friendCount = mapMsg.getInt("userFriendCount");
                int statusesCount = mapMsg.getInt("statusesCount");
                try {
                    BasicDBObject userDoc = (BasicDBObject) idToCelebMap.get(userId);
                    if(inReplyTo == null || inReplyTo.isEmpty()){
                        //try to get it from tweet
                        try {
                            int ati = text.indexOf("@");
                            if(ati > 0){
                        int sp = (text+" ").indexOf(" ", ati);
                        int spc = text.indexOf("'", ati);
                        if(spc!= -1 && spc<sp) sp=spc;
                        String ats = (text+" ").substring(ati,sp);
                        if(ats.length()<15) inReplyTo = ats;}
                        } catch (Exception e) {
                        }

                    }
                    if (userDoc == null && inReplyTo!= null) {
                        inReplyTo = inReplyTo.toLowerCase();
                        if (celebMap.containsKey(inReplyTo)) {
                            fanTweetsCount++;
                            DBActions.insertFanTweet(tweetId, text, userId, userName, userScreenname, userImage, time, inReplyTo,inReplyToTweet);
                        }
                    }
                    if (userDoc != null) {
                        tweetsCount++;
                        DBActions.insertTweet(tweetId, text, userId, time,geo, inReplyTo,inReplyToTweet);
                        userDoc.put("userScreenName", userScreenname);
                        userDoc.put("userName", userName);
                        userDoc.put("profileImageUrl", userImage);
                        userDoc.put("description",description);
                        userDoc.put("followers", followCount);
                        userDoc.put("friends", friendCount);
                        userDoc.put("statuses", statusesCount);
                        userDoc.put("isVerified", isVerified);
                        userDoc.put("location", location);
                        idToCelebMap.remove(userId);
                        idToCelebMap.put(userId, userDoc);

                        String[] imageUrl = ProcessTweet.getImage(text);
                        if (!imageUrl[0].isEmpty()) {
                            DBActions.insertPic(tweetId, imageUrl, userId, userScreenname, userName, time);
                        }
                    }

                    long t = System.currentTimeMillis();
                    if (t - userRefreshTime > REFRESH_INTERVAL) {
                        DBActions.updateCelebs(idToCelebMap);
                        System.out.println("Refreshed userInfo, inserted:"+tweetsCount+"(celebs),"+fanTweetsCount+"(fans) tweets");
                        
                        userRefreshTime = t;
                    }
                } catch (Exception e) {
                    System.out.println("ex in deque data inserts:" + e);
                }

            } catch (Exception e) {
                System.out.println("Ex in deque:" + e);
                e.printStackTrace();
            }
        }

    }
}
