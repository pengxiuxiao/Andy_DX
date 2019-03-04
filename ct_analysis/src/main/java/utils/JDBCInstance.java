package utils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author: pxx
 * @Date: 2019/3/5 0:00
 * @Version 1.0
 */
public class JDBCInstance {
    private static Connection connection = null;

    public JDBCInstance() {
    }

    /**
     * 获取连接实例
     * @return
     */
    public static Connection gentInstance(){
        try {
            if (connection == null || connection.isClosed()) {
                connection = JDBCUtils.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
