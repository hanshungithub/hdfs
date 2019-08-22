package cn.hassan.hdfsres.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowSortPartitioner extends Partitioner<FlowBean, Text> {
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        int partitioner = 4;
        String preNum = text.toString().substring(0, 3);
        if ("136".equals(preNum)) {
            return 0;
        } else if ("137".equals(preNum)) {
            return 1;
        } else if ("138".equals(preNum)) {
            return 2;
        } else if ("139".equals(preNum)) {
            return 3;
        }
        return 4;
    }
}
