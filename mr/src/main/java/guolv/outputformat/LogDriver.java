package guolv.outputformat;

import guolv.partition.LogsPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: LogDriver
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 16:38
 * @Description:
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动类jar
        job.setJarByClass(LogDriver.class);

        //3.设置maper reduce class
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);


        //4.maper输出键值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.reduce输出键值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置outputformate
        job.setOutputFormatClass(LogOutputFormat.class);

        //6.文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("e:/mr/logs/in"));
        FileOutputFormat.setOutputPath(job, new Path("e:/mr/logs/out"));

        // 7.提交任务
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs ? 0 : 1);
    }
}
