package flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.lang.reflect.Parameter;

/**
 * @ClassName: FlowSortPartition
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 13:54
 * @Description:
 */
public class FlowSortPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int i) {

        //获取手机号前三位
        String phone = value.toString().substring(0,3);

        //根据手机号前三位分区
        if ("135".equals(phone)) {
            return 0;
        }else if ("137".equals(phone)) {
            return 1;
        }else if("138".equals(phone)) {
            return 2;
        }else if("139".equals(phone)) {
            return 3;
        }
        return 4;
    }
}
