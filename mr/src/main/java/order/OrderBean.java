package order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: OrderBean
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 15:14
 * @Description:
 */
public class OrderBean implements WritableComparable<OrderBean> {

    //id
    private int id;

    //price
    private double price;

    public OrderBean() {

    }

    public OrderBean(int id, double price) {
        this.id = id;
        this.price = price;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {

        int rs;
        if (this.id > o.getId()) {
            rs = 1;
        } else if (this.id < o.getId()) {
            rs = -1;
        } else {
          rs = this.price > o.getPrice() ? -1 : 1;
        }

        return rs;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeDouble(price);

    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return id + "\t" + price;
    }
}
