package inputfmt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: SequenceFileDriver
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 11:44
 * @Description:
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动类jar
        job.setJarByClass(SequenceFileDriver.class);

        //3.设置maper reduce class
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //设置自定义的读取方式
        job.setInputFormatClass(FuncFileInputFormat.class);

        //设置默认的输出方式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //4.maper输出键值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //5.reduce输出键值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //6.文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("e:/mr/file/in"));
        FileOutputFormat.setOutputPath(job, new Path("e:/mr/file/out"));

        // 7.提交任务
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs ? 0 : 1);
    }
}
