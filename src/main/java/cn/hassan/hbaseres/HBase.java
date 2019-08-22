package cn.hassan.hbaseres;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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

	@After
	public void after() {
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void deleteTest() throws IOException {
		Table table = connection.getTable(TableName.valueOf("test"));
		Delete delete = new Delete("1002".getBytes());
		//在delete对象中指定要删除的列族-列名称
		//delete.addColumn("cf".getBytes(), "name".getBytes());
		table.delete(delete);
	}

	@Test
	public void scanTest() throws IOException {
		Table table = connection.getTable(TableName.valueOf("test"));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while (iterator.hasNext()) {
			Result next = iterator.next();
			List<Cell> cells = next.listCells();
			printValue(cells);
		}
	}

	@Test
	public void getTest() throws IOException {
		Table table = connection.getTable(TableName.valueOf("test"));
		Get get = new Get("1000".getBytes());
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		printValue(cells);
	}

	private void printValue(List<Cell> cells) {
		for (Cell cell : cells) {
			byte[] rowArray = cell.getRowArray();
			byte[] familyArray = cell.getFamilyArray();
			byte[] qualifierArray = cell.getQualifierArray();
			byte[] valueArray = cell.getValueArray();
			//按指定位置截取，获取rowArray、familyArray、qualifierArray、valueArray
			System.out.print(new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
			System.out.print(" "+new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out.print(":" + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println(" " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
		}
	}

	@Test
	public void addTest() throws IOException {
		Table table = connection.getTable(TableName.valueOf("test"));

		Put put = new Put(Bytes.toBytes("1002"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
		put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("25"));

		table.put(put);
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
