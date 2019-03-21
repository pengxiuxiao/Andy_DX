package order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName: OrderGroupComparator
 * @Description:
 * @Author: pxx
 * @Date: 2019/3/20 18:00
 * @Description:
 */
public class OrderGroupComparator extends WritableComparator {

    //构造函数必须加
    public OrderGroupComparator() {
        super(OrderBean.class,true);
    }

    //重写比较

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int rs;
        if (aBean.getId() > bBean.getId()) {
            rs = 1;
        } else if (aBean.getId() < bBean.getId()) {
            rs = -1;
        } else {
            rs = 0;
        }
        return rs;
    }
}
