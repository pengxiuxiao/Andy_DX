package guolv.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: LogsReducer
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 15:31
 * @Description:
 */
public class LogsReducer extends Reducer<Text, LogsBean, Text, LogsBean> {
    @Override
    protected void reduce(Text key, Iterable<LogsBean> values, Context context) throws IOException, InterruptedException {


        context.write(key, values.iterator().next());
    }
}
