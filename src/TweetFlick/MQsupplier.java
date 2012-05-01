package TweetFlick;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import Utilities.MQUtilities;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

/**
 *
 * @author Karthik
 */
public class MQsupplier {
    static int count=0;

    javax.jms.Connection connection = null;
    Session session = null;
    javax.jms.Queue queue = null;
    MessageProducer supplier = null;

    public MQsupplier() {
        init();
    }

    void init() {
        if (connection != null) {
            try {
                session.close();
            } catch (JMSException je) {
            }
            try {
                connection.close();
            } catch (Exception e) {
            }
        }
        try {
            MQUtilities mqutils = new MQUtilities();
            connection = mqutils.getConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue("TWEETS_QUEUE");
            supplier = session.createProducer(queue);
            supplier.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            supplier.setTimeToLive(1000 * 60 * 200);
            connection.start();
        } catch (Exception e) {
            System.err.println("Could not initiate Message Queue:" + e);
            e.printStackTrace();
        }

    }

    public void pushToMQ(Status status) {
        try {
            User user = status.getUser();
            MapMessage tweet = null;

            tweet = session.createMapMessage();
//            tweet.setJMSExpiration(System.currentTimeMillis() + (5 * 60 * 60 * 1000)); //if not dequeued for 5 hrs, ignore it.
            tweet.setString("text", status.getText());
            tweet.setLong("tweetId", status.getId());
            tweet.setLong("time", status.getCreatedAt().getTime());
            tweet.setString("inReplyTo", status.getInReplyToScreenName());
            tweet.setLong("inReplyToTweet",status.getInReplyToStatusId());
            String geo = "";
            try {
                geo = status.getGeoLocation().getLatitude()+"-"+status.getGeoLocation().getLongitude();
            } catch (Exception e) {
            }
            tweet.setString("geo",geo);
            tweet.setLong("userId", user.getId());
            tweet.setString("userName", user.getName());
            tweet.setString("userScreenname", user.getScreenName());
            tweet.setString("userImgurl", user.getProfileImageURL().toString());
            tweet.setString("description",user.getDescription());
            tweet.setString("userLocation", user.getLocation());
            tweet.setBoolean("isVerified", user.isVerified());
            tweet.setInt("userFollowCount", user.getFollowersCount());
            tweet.setInt("userFriendCount", user.getFriendsCount());
            tweet.setInt("statusesCount", user.getStatusesCount());
            supplier.send(tweet);
            count++;
            if(count%100 == 0){
                System.out.println("Supplier sent "+count+" messages");
            }
        } catch (Exception e) {
            System.out.println("Could not send to queue:" + e);
            init();
            try {
                Thread.sleep(2000);
            } catch (Exception ex) {
            }

        }


    }
}
