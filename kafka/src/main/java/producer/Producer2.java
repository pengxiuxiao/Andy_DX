package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author: pxx
 * @Date: 2019/3/25 22:58
 * @Version 1.0
 */
public class Producer2 {
    public static void main(String[] args) {
        // 1.配置生产者属性（指定多个参数）
        Properties prop = new Properties();

        // 参数配置
        // kafka节点的地址
        prop.put("bootstrap.servers", "192.168.2.130:9092");
        // 发送消息是否等待应答
        prop.put("acks", "all");
        // 配置发送消息失败重试
        prop.put("retries", "0");
        // 配置批量处理消息大小
        prop.put("batch.size", "10241");
        // 配置批量处理数据延迟
        prop.put("linger.ms", "5");
        // 配置内存缓冲大小
        prop.put("buffer.memory", "12341235");
        // 消息在发送前必须序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //指定分区
        prop.put("partitioner.class", "producer.Partition1");


        //2.实例化producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //3.发送消息
        for(int i = 0;i<99;i++) {
            producer.send(new ProducerRecord<String, String>("yuandan", "pxx" + i), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //如果metadata不为null 拿到当前的数据偏移量与分区
                    if(metadata != null) {
                        System.out.println(metadata.topic() + "----" + metadata.offset() + "----" + metadata.partition());
                    }
                }
            });
        }

        //4.关闭资源
        producer.close();
    }
}
