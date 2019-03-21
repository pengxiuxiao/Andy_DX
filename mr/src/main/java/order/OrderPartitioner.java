package order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: OrderPartitioner
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 15:36
 * @Description:
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable text, int numReduceTasks) {

        return (orderBean.getId() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
