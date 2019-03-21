package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: OrderMaper
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 15:23
 * @Description:
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.按行读取数据
        String line = value.toString();

        //2.切割
        String[] fields = line.split("\t");

        //3.取出关键数据封装
        int id = Integer.parseInt(fields[0]);
        Double price = Double.parseDouble(fields[2]);

        //4.写出
        context.write(new OrderBean(id, price), NullWritable.get());
    }
}
