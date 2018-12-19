package cn.hassan.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMpper extends Mapper<LongWritable, Text, Text, TableBean> {

    TableBean v = new TableBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //区分两张表
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        String lines = value.toString();

        if (name.contains("order")) {
            //定义函数抽取
            String[] fields = lines.split(" ");
            //属性值必须都设置
            v.setOrderId(fields[0]);
            v.setpId(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setpName("");
            v.setFlag((byte) 0);

            k.set(fields[1]);
        } else {
            //定义函数抽取
            String[] fields = lines.split(" ");
            //属性值必须都设置
            v.setOrderId("");
            v.setpId(fields[0]);
            v.setAmount(0);
            v.setpName(fields[1]);
            v.setFlag((byte) 1);

            k.set(fields[0]);
        }

        context.write(k, v);
    }
}
