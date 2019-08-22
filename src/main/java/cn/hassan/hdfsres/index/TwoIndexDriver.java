package cn.hassan.hdfsres.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created with idea
 * Author: hss
 * Date: 12/21/2018 9:13 AM
 * Description:
 */
public class TwoIndexDriver {

	public static void main(String[] args) throws Exception {

		args = new String[]{"F:/webupload/twoindex","F:/webupload/indexout1"};

		Configuration cfg = new Configuration();

		Job job = Job.getInstance(cfg);

		job.setJarByClass(TwoIndexDriver.class);
		job.setMapperClass(TwoIndexMapper.class);
		job.setReducerClass(TwoIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
