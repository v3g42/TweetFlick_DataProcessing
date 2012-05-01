/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Utilities;

import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

/**
 *
 * @author Karthik
 */
public class SettingsMap {
     static Properties settings = new Properties();
    static String resource = "Utilities.settings";

    static {
        try {
            final ResourceBundle rb = ResourceBundle.getBundle(resource);
            Set <String> keys = rb.keySet();
            for(String key : keys){
                settings.put(key,rb.getString(key));
            }
        } catch (Exception e) {
            System.err.println("Could not load settings in SettingsMap-"+ e.getMessage());
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        return settings.getProperty(key);
    }

}
