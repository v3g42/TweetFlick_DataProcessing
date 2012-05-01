/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Utilities;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 *
 * @author Karthik
 */
public class MongoUtils {

   public static String getCurrentTime() {
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        return "" + dateFormatGmt.format(new java.util.Date());
    }
}
