package flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: FlowSortDriver
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 11:52
 * @Description:
 */
public class FlowSortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置driver类信息
        job.setJarByClass(FlowSortDriver.class);

        //3.设置mapper reduce类信息
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //4.设置mapper输出键 值类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置reduce输出键 值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置partition
        job.setPartitionerClass(FlowSortPartitioner.class);
        job.setNumReduceTasks(5);

        //6.设置文件读取 输出目录
        FileInputFormat.setInputPaths(job,new Path("e:/flow/out"));
        FileOutputFormat.setOutputPath(job, new Path("e:/flow/out2"));
        //7.启动任务
        boolean rs = job.waitForCompletion(true);
        System.out.println(rs);

    }
}
