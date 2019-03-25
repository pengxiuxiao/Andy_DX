package producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author: pxx
 * @Date: 2019/3/25 23:02
 * @Version 1.0
 */
public class Partition1 implements Partitioner {

    //分区逻辑
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 1;
    }

    //关闭资源
    @Override
    public void close() {

    }

    //设置
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
