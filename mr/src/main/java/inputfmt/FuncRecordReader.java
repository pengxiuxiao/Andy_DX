package inputfmt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName: FuncRecordReader
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/22 10:08
 * @Description:
 */
public class FuncRecordReader extends RecordReader<NullWritable, BytesWritable> {

    boolean isProcess = false;
    FileSplit split;
    Configuration conf;
    BytesWritable value = new BytesWritable();

    //初始化
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化切片信息
        this.split = (FileSplit) inputSplit;

        //初始化配置文件
        this.conf = taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isProcess) {
            //1.根据切片长度来创建缓冲区
            byte[] buf = new byte[(int) split.getLength()];
            FSDataInputStream fis = null;
            FileSystem fs = null;

            try {
                //2.获取路径
                Path path = split.getPath();

                //3.根据路径获取文件系统
                fs = path.getFileSystem(conf);

                //4.拿到输入流
                fis = fs.open(path);

                //5.拷贝数据
                IOUtils.readFully(fis, buf, 0, buf.length);

                //6.拷贝数据到最终的输出
                value.set(buf, 0, buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fs);
            }

            isProcess = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
