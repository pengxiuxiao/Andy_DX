package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/19 11:24
 * @Version 1.0
 * 3631279850362	13726130503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	www.itstaredu.com	教育网站	24	27	299	681	200
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.拿到数据
        String line = value.toString();
        //2. 分割数据
        String[] fields = line.split("\t");

        //3.封装对象
        String phoneN = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long dfFlow = Long.parseLong(fields[fields.length - 2]);

        //4.输出到reduce 13726130503 299 681
        context.write(new Text(phoneN), new FlowBean(upFlow, dfFlow));


    }
}
