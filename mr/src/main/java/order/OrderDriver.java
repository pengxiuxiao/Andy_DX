package order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: OrderDriver
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 15:42
 * @Description:
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.初始化
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动类jar
        job.setJarByClass(OrderDriver.class);

        //3.设置maper reduce calss
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4.设置maper 输出键值
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置reduce输出键值
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置reduce端的分组  --
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        //7.设置自定义分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        //8.设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("e:/mr/order/in"));
        FileOutputFormat.setOutputPath(job, new Path("e:/mr/order/out"));

        //9.启动
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs);


    }

}
