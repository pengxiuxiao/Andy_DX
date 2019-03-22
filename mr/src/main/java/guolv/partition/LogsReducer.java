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
public class LogsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //加一个换行
        String line = key.toString() + "333";
        System.out.println(line);
        context.write(new Text(line), NullWritable.get());
    }
}
