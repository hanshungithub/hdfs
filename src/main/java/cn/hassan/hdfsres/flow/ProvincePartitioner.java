package cn.hassan.hdfsres.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
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
        return partitioner;
    }

}
