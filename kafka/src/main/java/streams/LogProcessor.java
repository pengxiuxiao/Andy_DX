package streams;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Author: pxx
 * @Date: 2019/3/25 23:41
 * @Version 1.0
 */
public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    //初始化
    @Override
    public void init(ProcessorContext context) {
        //传输
        this.context = context;
    }

    //业务逻辑
    @Override
    public void process(byte[] key, byte[] value) {
        //1.拿到消息数据 转成字符串
        String message = new String(value);

        //2.如果包含 - 去掉
        if (message.contains("-")) {
            System.out.println(message);
            //3.把-去掉
            message = message.split("-")[1];
        }

        //4.发送数组
        context.forward(key, message.getBytes());

    }

    //释放资源
    @Override
    public void close() {

    }
}
