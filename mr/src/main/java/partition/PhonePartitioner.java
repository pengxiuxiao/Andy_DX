package partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: pxx
 * @Date: 2019/3/20 9:53
 * @Version 1.0
 */
public class PhonePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        //1.获取手机号前三位
        String  phoneNum = key.toString().substring(0,3);

        //2.分区
        int partitioner = 4;

        if ("135".equals(phoneNum)) {
            return 0;
        }else if ("137".equals(phoneNum)) {
            return 1;
        }else if("138".equals(phoneNum)) {
            return 2;
        }else if("139".equals(phoneNum)) {
            return 3;
        }else{
            return partitioner;
        }
    }
}
