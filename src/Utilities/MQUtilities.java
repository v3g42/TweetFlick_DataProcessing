/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Utilities;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *
 * @author Karthik
 */
public class MQUtilities {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(SettingsMap.get("MQ_USERNAME"), SettingsMap.get("MQ_PWD"), SettingsMap.get("MQ_URL"));

    public javax.jms.Connection getConnection() {
        try {
            javax.jms.Connection connection = null;
            connection = connectionFactory.createConnection();
            connection.start();
            return connection;
        } catch (Exception e) {
            System.out.println("Problem creating connection. Reason:" + e.getMessage());
        }
        return null;
    }
}
