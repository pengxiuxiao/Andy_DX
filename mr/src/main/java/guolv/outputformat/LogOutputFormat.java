package guolv.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: LogOutputFormat
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 16:41
 * @Description:
 */
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        LogRecordWrite logRecordWrite = new LogRecordWrite(taskAttemptContext);
        return logRecordWrite;
    }
}
