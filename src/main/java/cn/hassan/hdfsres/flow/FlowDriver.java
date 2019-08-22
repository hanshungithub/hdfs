package cn.hassan.hdfsres.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取configuration对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2 设置jar包路径
        job.setJarByClass(FlowDriver.class);

        //3 管理mapper和reducer类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6 设置输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
