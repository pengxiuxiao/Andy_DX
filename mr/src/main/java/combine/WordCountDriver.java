package combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/18 23:55
 * @Version 1.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        //2.获取driver类的jar包
        job.setJarByClass(WordCountDriver.class);

        //3.获取自定义的mapper与reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置map输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置reduce输出的数据类型 （最终的数据类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定运行的inputformat方式 默认的方式是textinputformat(小文件优化)
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);//最大4M
        CombineTextInputFormat.setMinInputSplitSize(job, 3145728);//最小3M

        //6.设置输入存在的路径与处理后的结果路径
        FileInputFormat.setInputPaths(job, new Path("/pxx"));
        FileOutputFormat.setOutputPath(job, new Path("/pxx/0319out"));

        //7.提交任务
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs? 0 : 1);
    }
}
