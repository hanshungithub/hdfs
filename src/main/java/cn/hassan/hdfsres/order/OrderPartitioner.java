package cn.hassan.hdfsres.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (int) ((orderBean.getOrderId() & Integer.MAX_VALUE) % numPartitions);
    }
}
