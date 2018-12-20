package cn.demo.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMpper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        boolean result = parseLog(line, context);

        if (!result) {
            return;
        }
        k.set(line);

        context.write(k,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            context.getCounter("logmap", "parseLog_true").increment(1);
            return true;
        }else {
            context.getCounter("logmap", "parseLog_false").increment(1);
            return false;
        }
    }
}
