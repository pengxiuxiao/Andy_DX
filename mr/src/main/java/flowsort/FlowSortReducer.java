package flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: FlowSortReducer
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 11:49
 * @Description:
 */
public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
