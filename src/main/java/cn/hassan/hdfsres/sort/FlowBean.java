package cn.hassan.hdfsres.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long totalFlow;

    /**
     * 空参数构造方法 反射用
     */
    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);

    }

    /**
     * 反序列化方法
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

	/**
	 *   return s1.age > s2.age ? 1 : -1; 返回值为1时，也就是说 s1的值大于s2的值时 ，compareTo是按照升序（由小到大）排序的！
	 *  当返回值为-1时，也就是说 s1的值小于s2的值时 ，compareTo是按照降序（由大到小）排序的
	 */
	public int compareTo(FlowBean o) {
		return this.totalFlow > o.getTotalFlow() ? 1 : -1;
    }
}
