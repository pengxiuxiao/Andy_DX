package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author: pxx
 * @Date: 2019/3/25 23:26
 * @Version 1.0
 */
public class TimeInterceptor implements ProducerInterceptor<String, String > {

    //配置信息
    @Override
    public void configure(Map<String, ?> configs) {
    }
    //业务逻辑
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        return new ProducerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.key(),
                System.currentTimeMillis() + "-" + record.value());
    }
    //发送失败调用
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }
    //关闭资源
    @Override
    public void close() {
    }
}
