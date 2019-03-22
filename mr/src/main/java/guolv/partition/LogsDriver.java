package guolv.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: LogsDriver
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 15:32
 * @Description:
 */
public class LogsDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动类jar
        job.setJarByClass(LogsDriver.class);

        //3.设置maper reduce class
        job.setMapperClass(LogsMapper.class);
        job.setReducerClass(LogsReducer.class);


        //4.maper输出键值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.reduce输出键值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置partition
        job.setPartitionerClass(LogsPartitioner.class);
        job.setNumReduceTasks(2);

        //6.文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("e:/mr/logs/in"));
        FileOutputFormat.setOutputPath(job, new Path("e:/mr/logs/out"));

        // 7.提交任务
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs ? 0 : 1);
    }

}
