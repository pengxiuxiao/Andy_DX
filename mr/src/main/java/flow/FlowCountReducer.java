package flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/19 11:47
 * @Version 1.0
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.相同的手机号 流量使用再次汇总
        long upFlow_sum = 0;
        long dfFlow_sum = 0;
        //2.累加
        for (FlowBean f : values) {
            upFlow_sum += f.getUpFlow();
            dfFlow_sum =+ f.getDfFlow();
        }
        FlowBean rs = new FlowBean(upFlow_sum, dfFlow_sum);

        //3.输出
        context.write(key, rs);
    }
}
