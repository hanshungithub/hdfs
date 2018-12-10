package cn.hassan.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper 类
 * LongWritable 对应读取文件的第一行作为key（key位偏移量）
 * Text 读取进来的值
 * Text map输出的key
 * FlowBean map输出的value
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split(" ");

        String phone = fields[1];

        long upFlow = Long.parseLong(fields[fields.length - 3]);

        long downFlow = Long.parseLong(fields[fields.length - 2]);

        v.set(upFlow, downFlow);
        k.set(phone);

        context.write(k, v);
    }
}
