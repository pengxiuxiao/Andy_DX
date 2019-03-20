package combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/18 23:48
 * @Version 1.0
 *
 * 汇总 <hello,4> <hunter,1> <henshuai,2>
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.统计单词出现的次数
        int sum = 0;

        //2.累加求和
        for (IntWritable count : values) {
            //拿到值累加
            sum += count.get();
        }

        //3.结果输出
        context.write(key,new IntWritable(sum));
    }
}
