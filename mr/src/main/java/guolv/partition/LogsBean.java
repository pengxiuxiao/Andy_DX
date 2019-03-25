package guolv.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: LogsBean
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/25 09:33
 * @Description:
 */
public class LogsBean implements Writable {

    private String log_id;


    public LogsBean(String log_id) {
        this.log_id = log_id;
    }

    public String getLog_id() {
        return log_id;
    }

    public void setLog_id(String log_id) {
        this.log_id = log_id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(log_id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        log_id = in.readUTF();
    }

    @Override
    public String toString() {
        return log_id + "\t" ;
    }
}
