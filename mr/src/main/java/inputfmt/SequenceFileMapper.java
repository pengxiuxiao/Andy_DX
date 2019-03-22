package inputfmt;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @ClassName: SquenceFileMapper
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 11:33
 * @Description:
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.拿到切片
        FileSplit split = (FileSplit) context.getInputSplit();

        //2.获取路径
        Path path = split.getPath();

        //3.即带路径又带名称
        text.set(path.toString());

    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(text, value);
    }
}
