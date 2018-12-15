package cn.hassan.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private BytesWritable value = new BytesWritable();
    private boolean isProcess;

    private FileSplit split;
    private Configuration configuration;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        FileSystem fs = null;
        FSDataInputStream fis = null;
        if (!isProcess) {
            try {
                byte[] buf = new byte[(int) split.getLength()];
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                fis = fs.open(path);
                IOUtils.readFully(fis, buf, 0, buf.length);
                value.set(buf, 0, buf.length);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(fs);
                IOUtils.closeStream(fis);
            }
            isProcess = true;
            return true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return isProcess ? 1 : 0;
    }

    public void close() throws IOException {

    }
}
