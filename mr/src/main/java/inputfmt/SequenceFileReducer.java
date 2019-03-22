package inputfmt;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: SequnceFileReducer
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 11:42
 * @Description:
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value : values) {
            context.write(key, value);
        }
    }
}
