package cn.hassan.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with idea
 * Author: hss
 * Date: 12/21/2018 9:09 AM
 * Description:
 */
public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder builder = new StringBuilder();

		values.forEach(e -> {
			builder.append(e.toString().replace("\t", " ---> ")).append(" ");
		});

		context.write(key,new Text(builder.toString()));
	}
}
