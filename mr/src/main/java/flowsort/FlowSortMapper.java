package flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: FlowSortMapper
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 11:29
 * @Description:
 */
public class FlowSortMapper extends Mapper<LongWritable,Text, FlowBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.按行取数据
        String line = value.toString();

        //2.切割
        String[] fields = line.split("/t");

        //3.取出关键字段
        long upFlow = Long.parseLong(fields[1]);
        long dfFlow = Long.parseLong(fields[2]);

        //4.写出去
        context.write(new FlowBean(upFlow,dfFlow), new Text(fields[0]));
    }

}
