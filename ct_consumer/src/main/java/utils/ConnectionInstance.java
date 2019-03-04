package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/2/17 23:50
 * @Version 1.0
 */
public class ConnectionInstance {
    private static Connection conn;

    public static synchronized Connection getConnection(Configuration configuration) {
        try {
            if (conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection(configuration);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
