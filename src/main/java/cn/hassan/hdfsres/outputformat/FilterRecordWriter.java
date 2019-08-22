package cn.hassan.hdfsres.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private Configuration configuration;
    private FSDataOutputStream hassanfos;
    private FSDataOutputStream otherfos;

    public FilterRecordWriter(TaskAttemptContext job) {
        this.configuration = job.getConfiguration();
        try {
            FileSystem fs = FileSystem.get(configuration);
            hassanfos = fs.create(new Path("e:/hassan.log"));
            otherfos = fs.create(new Path("e:/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("hassan")) {
            hassanfos.write(key.getBytes());
        }else {
            otherfos.write(key.getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (hassanfos != null) {
            hassanfos.close();
        }

        if (otherfos != null) {
            otherfos.close();
        }
    }
}
