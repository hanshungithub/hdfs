package cn.hassan.hbaseres;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with idea
 * Author: hss
 * Date: 2019/8/22 11:17
 * Description:
 */
public class HBase {

	private Configuration configuration;
	private Connection connection;

	@Before
	public void before() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.master", "hadoop101:16010");
		configuration.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Test
	public void createTest() {
		createTable("hassan",new String[]{"column1"});
	}


	private void createTable(String tableName,String[] columns) {
		try {
			Admin admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("表已经存在");
			}else {
				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
				for (String column : columns) {
					HColumnDescriptor columnDescriptor = new HColumnDescriptor(column);
					descriptor.addFamily(columnDescriptor);
				}
				admin.createTable(descriptor);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
