package streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

/**
 * @Author: pxx
 * @Date: 2019/3/25 23:40
 * @Version 1.0
 * 需求：对数据进行清洗
 * 思路：pxx-henshaui 把 - 去掉
 *
 */
public class Application {
    public static void main(String[] args) {
        //1.定义主题 发送到另一个主题 数据清洗
        String oneTopic = "t1";
        String twoTopic = "t2";

        //2.设置属性
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.130:9092,192.168.2.131:9092,192.168.2.132:9092");

        //3.实例对象
        StreamsConfig config = new StreamsConfig(prop);

        //4.流计算拓扑
        Topology buider = new Topology();

        //5.定义kafka组件数据源
        buider.addSource("Source",oneTopic).addProcessor("Processor", new ProcessorSupplier() {
            @Override
            public Processor get() {
                return new LogProcessor();
            }
            //从哪里来
        }, "Source")
                //到哪里去
                .addSink("Sink", twoTopic, "Processor");

        //6.实例化kafkastream
        KafkaStreams kafkaStreams = new KafkaStreams(buider, prop);
        kafkaStreams.start();


    }
}
