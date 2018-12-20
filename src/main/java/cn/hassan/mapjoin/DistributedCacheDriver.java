package cn.hassan.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class DistributedCacheDriver {

    public static void main(String[] args) throws Exception {

        args = new String[]{"E:/hadoop/order","E:/hadoop/orderout1"};

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI("file:///e:/hadoop/order/prod.txt"));
        job.setNumReduceTasks(0);

        boolean finish = job.waitForCompletion(true);

        System.exit(finish ? 0 : 1);
    }
}
