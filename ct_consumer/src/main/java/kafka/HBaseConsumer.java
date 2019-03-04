package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/**
 * @Author: pxx
 * @Date: 2019/2/17 23:59
 * @Version 1.0
 */
public class HBaseConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));
        HBaseDAO hd = new HBaseDAO();
        while (true){
            ConsumerRecords<String,String> record = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> cr : record) {
                String ori = cr.value();
                System.out.println(ori);
                hd.put(ori);
            }
        }
    }
}
