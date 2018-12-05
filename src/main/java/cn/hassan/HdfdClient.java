package cn.hassan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class HdfdClient {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //需要在VM 加参数-DHADOOP_USER_NAME=admin
        //conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
        //FileSystem fileSystem = FileSystem.get(conf);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), conf, "admin");

        fileSystem.copyFromLocalFile(new Path("e:/a.txt"), new Path("/a1.txt"));

        fileSystem.close();
    }

    @Test
    public void getFileFromHDFS() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration, "admin");

        fileSystem.copyToLocalFile(false,new Path("hdfs://hadoop101:9000/a.txt"), new Path("e:/aa.txt"),true);
        fileSystem.close();
    }
}
