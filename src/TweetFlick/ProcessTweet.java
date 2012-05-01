/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package TweetFlick;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 *
 * @author Karthik
 */
public class ProcessTweet {

//     http://img.ly/show/thumb/3de
//http://yfrog.com/gzkbryhj:small
//http://twitpic.com/show/thumb/1e10q
    public static void main(String[] args) {
       
        String t = "Check and Reply Friends! Howz the Poster http://t.co/R7Kjl9F";
        String img = getImage(t)[0];
        System.out.println("" + img);
    }

    static String[] getImage(String text) {
        String[] urls = new String[3];
        for (int i = 0; i < 3; i++) {
            urls[i] = "";
        }
        try {
            text = text + " ";
            int tco = text.indexOf("t.co/");
            String t = "";
            if (tco != -1) {
                t = text.substring(tco, text.indexOf(" ", tco));
                if(!t.isEmpty()){
                    String redicted = getRedirect("http://"+t);
                      String[] tcos = getImage(redicted);
//                      System.out.println("From tco:"+tcos[0]+".."+redicted);
                return tcos;
                }
              
            }
            int twitpic = text.indexOf("twitpic.com/");
            int yfrog = text.indexOf("yfrog.com/");
            int imgly = text.indexOf("img.ly/");
            int index = -1;
            if (twitpic != -1) {
                index = twitpic;
                t = text.substring(index + 12, text.indexOf(" ", index));
                urls[0] = "http://twitpic.com/" + t;
                urls[1] = "http://twitpic.com/show/thumb/" + t;
                urls[2] = "http://twitpic.com/show/large/" + t;
            } else if (yfrog != -1) {
                index = yfrog;
                t = text.substring(index + 10, text.indexOf(" ", index));
                urls[0] = "http://yfrog.com/" + t;
                urls[1] = "http://yfrog.com/" + t + ":small";
                urls[2] = "";
            } else if (imgly != -1) {
                index = imgly;
                t = text.substring(index + 8, text.indexOf(" ", index));
                urls[0] = "http://img.ly/" + t;
                urls[1] = "http://img.ly/show/thumb/" + t;
                urls[2] = "http://img.ly/show/full/" + t;
            }
        } catch (Exception e) {
            System.out.println("Ex in getting image:" + e);
            e.printStackTrace();
        }

        return urls;
    }

    static String getRedirect(String link) {
//        String link="http://t.co/R7Kjl9F";
        try {
//            URL url = new URL(link);
            HttpURLConnection con = (HttpURLConnection) (new URL(link).openConnection());
            con.setInstanceFollowRedirects(false);
            con.connect();
//            int responseCode = con.getResponseCode();
//            System.out.println(responseCode);
            String location = con.getHeaderField("Location");
            return location;
        } catch (Exception e) {
        }
        return "";
    }
}
