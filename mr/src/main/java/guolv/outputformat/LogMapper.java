package guolv.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: LogsMapper
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 15:21
 * @Description:
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.按行读取数据
        String line = value.toString();

        //2.数据处理
        //3.写出数据
        context.write(value, NullWritable.get());
    }
}
