package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: WordCountCombiner
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 14:22
 * @Description:
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.取值
        int count = 0;
        for (IntWritable value : values) {
            //2.累加
            count += value.get();
        }

        //3.写出
        context.write(key, new IntWritable(count));
    }
}
