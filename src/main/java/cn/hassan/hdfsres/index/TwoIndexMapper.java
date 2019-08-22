package cn.hassan.hdfsres.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with idea
 * Author: hss
 * Date: 12/21/2018 9:05 AM
 * Description:
 */
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split("--");

		k.set(fields[0]);
		v.set(fields[1]);

		context.write(k, v);
	}
}
