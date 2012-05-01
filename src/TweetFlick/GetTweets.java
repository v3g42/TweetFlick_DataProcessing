package TweetFlick;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Karthik
 */
import java.util.Properties;
import Utilities.SettingsMap;
import Utilities.MQUtilities;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.PropertyConfiguration;

public class GetTweets {

    static Properties props = new Properties();
    static TwitterStreamFactory tsfactory = null;
    static TwitterStream twitterStream = null;
    static MQsupplier mqSupplier = new MQsupplier();
    static long lastConnecttime;
    static final int RECONNECT_INTERVAL = 15*60*1000;
    static int count=0;
    public static void main(String[] args) {
        initTwitter();
        startStreaming();
    }

    static void initTwitter() {
        props.put("oauth.accessToken", SettingsMap.get("TWITTER_ACCESS_TOKEN"));
        props.put("oauth.accessTokenSecret", SettingsMap.get("TWITTER_ACCESS_TOKEN_SECRET"));
        props.put("oauth.consumerKey", SettingsMap.get("TWITTER_CONSUMER_KEY"));
        props.put("oauth.consumerSecret", SettingsMap.get("TWITTER_CONSUMER_SECRET"));
    }

    static void reconnect() {

        try {
            twitterStream.shutdown();
            System.out.println(new java.util.Date()+":gonna sleep in reconnect");
            Thread.sleep(1000 * 5);
            startStreaming();
        } catch (Exception e) {
            System.out.println("Error in shutting down in reconnect method:" + e);
            startStreaming();
        }
    }
    static StatusListener listener = new StatusListener() {

        public void onStatus(Status status) {
//            System.out.println("onstatus:"+status);
            System.out.println(new java.util.Date()+":tweet:"+ ++count);
            mqSupplier.pushToMQ(status);
            long t = System.currentTimeMillis();
            if(t - lastConnecttime  > RECONNECT_INTERVAL){
                reconnect();
            }
        }

        public void onDeletionNotice(StatusDeletionNotice sdn) {
            System.out.println("Deletion notice:" + sdn);
        }

        public void onTrackLimitationNotice(int i) {
            System.out.println("TrackLimited:::" + i);
        }

        public void onScrubGeo(long l, long l1) {
            System.out.println("Scrub geo:" + l + "," + l1);
        }

        public void onException(Exception excptn) {
            System.out.println("On ex:" + excptn);
//            excptn.printStackTrace();
            try {
                Thread.sleep(1000*30);
            } catch (Exception e) {
            }
            reconnect();
        }
    };

    static void startStreaming() {
        try {
            count=0;
            lastConnecttime = System.currentTimeMillis();
            tsfactory = new TwitterStreamFactory();
            Configuration conf = new PropertyConfiguration(props);

            AccessToken accessToken = new AccessToken(SettingsMap.get("TWITTER_ACCESS_TOKEN"), SettingsMap.get("TWITTER_ACCESS_TOKEN_SECRET"));
            Authorization auth = new OAuthAuthorization(conf);//conf, SettingsMap.get("TWITTER_CONSUMER_KEY"),SettingsMap.get("TWITTER_CONSUMER_SECRET"), accessToken);
            twitterStream = tsfactory.getInstance(auth);
            FilterQuery filterquery = new FilterQuery();
            long[] userids = DBActions.getUserIds();
            filterquery.follow(userids);

            Map<String,Long> celebs = DBActions.getCelebsForReply();
            Set<String> celebsScreenNames = celebs.keySet();
            String[] celebReply = new String[celebsScreenNames.size()];
            int i=0;
            for(String s:celebsScreenNames){
                celebReply[i++] = "@"+s;
            }
            filterquery.track(celebReply);
//            System.out.println("Following:"+userids.length+" and Tracking:"+celebReply.length);
//            for(long l:userids) System.out.println(""+l);
            twitterStream.addListener(listener);
            twitterStream.filter(filterquery);
            System.out.println(new java.util.Date() + ":Started Streaming for:"+userids.length+"and Tracking:"+celebReply.length);
           
        } catch (Exception e) {
            System.out.println(new java.util.Date() + ":Exception in start streaming:" + e);
//            reconnectIn5Mins();
        }
    }
}
