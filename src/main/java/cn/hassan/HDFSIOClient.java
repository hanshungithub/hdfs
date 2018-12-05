package cn.hassan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HDFSIOClient {

    @Test
    public void putFileToHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "admin");

        FileInputStream fileInputStream = new FileInputStream("e:/aa.txt");

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/aa.txt"));

        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);

    }

    @Test
    public void getFileFromHDFS() throws Exception{
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "admin");

        FSDataInputStream open = fileSystem.open(new Path("/aa.txt"));

        FileOutputStream outputStream = new FileOutputStream("e:/aaa.txt");

        IOUtils.copyBytes(open, outputStream, configuration);

        IOUtils.closeStream(open);
        IOUtils.closeStream(outputStream);

    }
}
