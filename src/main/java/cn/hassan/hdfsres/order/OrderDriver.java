package cn.hassan.hdfsres.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
    public static void main(String[] args) throws Exception {

		args = new String[]{"F:/webupload/groupcompator","F:/webupload/groupcompatorresult"};

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        //设置reduce的分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean finish = job.waitForCompletion(true);

        // 0-正常退出 程序正常执行结束退出 1-非正常退出 无论程序正常执行与否 都退出
        System.exit(finish ? 0 : 1);
    }
}
