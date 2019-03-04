package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: pxx
 * @Date: 2019/2/17 23:41
 * @Version 1.0
 */
public class PropertiesUtil {
    public static Properties properties = null;
    static {
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置文件的值
     * @param key
     * @return
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
