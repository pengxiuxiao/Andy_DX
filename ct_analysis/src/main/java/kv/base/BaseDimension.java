package kv.base;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: pxx
 * @Date: 2019/3/4 18:33
 * @Version 1.0
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {

    public abstract int compareTo(BaseDimension o);
    public abstract void write(DataOutput out) throws IOException;
    public abstract void readFields(DataInput in) throws IOException;

}
