package cn.hassan.hdfsres.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/hadoop/order/prod.txt"),"UTF-8"));
        String lines;
        while (StringUtils.isNotBlank(lines = reader.readLine())){
            String[] fields = lines.split(" ");
            pdMap.put(fields[0], fields[1]);
        }

        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String line = value.toString();
        String[] fields = line.split(" ");
        String pid = fields[1];
        String pName = pdMap.get(pid);
        if (name.contains("order")) {
            String newStr = fields[0] + " " + pName + fields[2];
            context.write(new Text(newStr),NullWritable.get());
        }
    }
}
