package guolv.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.security.Key;

/**
 * @ClassName: LogRecordWrite
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 16:43
 * @Description:
 */
public class LogRecordWrite extends RecordWriter<Text, NullWritable> {

    Configuration conf = null;
    FSDataOutputStream supadatalog = null;
    FSDataOutputStream otherlog = null;


    //1.定义数据输出路径
    public LogRecordWrite(TaskAttemptContext job) throws IOException {
        //获取配置信息
        conf = job.getConfiguration();

        //获取文件系统
        FileSystem fs = FileSystem.get(conf);

        //定义输出路径
        supadatalog = fs.create(new Path("e:/mr/logs/supadata.logs"));
        otherlog = fs.create(new Path("e:/mr/logs/otherlog.logs"));

    }


    //2.数据输出
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //根据key判断
        if (text.toString().contains("supadata")) {
            supadatalog.write(text.getBytes());
        }else{
            otherlog.write(text.getBytes());
        }

    }

    //3.关闭资源
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(null != supadatalog) {
            supadatalog.close();
        }

        if(null != otherlog) {
            otherlog.close();
        }
    }
}
