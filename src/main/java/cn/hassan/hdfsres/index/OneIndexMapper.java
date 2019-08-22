package cn.hassan.hdfsres.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new  IntWritable(1);

    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");

        for (String field : fields) {
            k.set(field + "--" + name);
            context.write(k, v);
        }
    }
}
