package cn.hassan.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private long orderId;
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(long orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readLong();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public int compareTo(OrderBean o) {
        int result;
        if (orderId > o.getOrderId()) {
            return 1;
        } else if (orderId < o.getOrderId()) {
            return -1;
        }else {
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }
}
