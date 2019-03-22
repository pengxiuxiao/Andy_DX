package guolv.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: LogsPartitioner
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 15:26
 * @Description:
 */
public class LogsPartitioner extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text key, NullWritable nullWritable, int i) {
        int rs = 0;
        if (key.toString().contains("supadata.com")) {
            rs = 1;
        }
        return rs;
    }
}
