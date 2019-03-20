package flowsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: FlowBean
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 11:32
 * @Description:
 */
public class FlowBean implements WritableComparable<FlowBean> {

    //定义属性
    private long upFlow;
    private long dfFlow;
    private long flowSum;

    //无参构造
    public FlowBean() {}

    //有参构造
    public FlowBean(long upFlow,long dfFlow) {

        this.upFlow = upFlow;
        this.dfFlow = dfFlow;
        this.flowSum = upFlow + dfFlow;
    }



    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDfFlow() {
        return dfFlow;
    }

    public void setDfFlow(long dfFlow) {
        this.dfFlow = dfFlow;
    }

    public long getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(long flowSum) {
        this.flowSum = flowSum;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dfFlow);
        out.writeLong(flowSum);

    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dfFlow = in.readLong();
        flowSum = in.readLong();
    }



    @Override
    public String toString() {
        return upFlow + "\t" + dfFlow + "\t" + flowSum;
    }


    @Override
    public int compareTo(FlowBean o) {
        return this.flowSum > o.getFlowSum() ? -1 : 1;
    }
}
