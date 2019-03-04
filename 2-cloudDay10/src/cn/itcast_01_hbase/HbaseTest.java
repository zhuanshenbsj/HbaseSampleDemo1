package cn.itcast_01_hbase;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

	/**
	 * 配置ss
	 */
	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;

	Random r = new Random();

	@Before
	public void init() throws Exception {
		/**
		 * HBaseConfiguration初始化会默认加载配置文件hbase-site.xml
		 * @throws Exception
		 */
		config = HBaseConfiguration.create();// 配置
		System.out.println(config.toString());
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(TableName.valueOf("phone_info"));
	}
	// @Before
	// public void init() throws Exception {
	// config = HBaseConfiguration.create(new Configuration());// 配置
	// config.set("hbase.zookeeper.quorum", "name1,data1,data2,data3");
	// // zookeeper地址
	// config.set("hbase.zookeeper.property.clientPort", "2181");
	// // zookeeper端口
	// System.out.println(config.toString());
	// connection = ConnectionFactory.createConnection(config);
	// table = connection.getTable(TableName.valueOf("phone_info"));
	// }

	/**
	 * 创建一个表
	 * 
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		// 创建表管理类
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin(); // hbase表管理
		// 创建表描述类
		TableName tableName = TableName.valueOf("phone_info"); // 表名称
		HTableDescriptor desc = new HTableDescriptor(tableName);
		// 创建列族的描述类
		HColumnDescriptor family = new HColumnDescriptor("info"); // 列族
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		family.setTimeToLive(86400);
		// 将列族添加到表中
		desc.addFamily(family);
		HColumnDescriptor family2 = new HColumnDescriptor("info2"); // 列族
		// 将列族添加到表中
		desc.addFamily(family2);

		try {
			// 创建表
			if (!admin.tableExists(tableName)) {
				admin.createTable(desc); // 创建表
				System.out.println("#### table:" + tableName.toString() + " create successful!");
			} else {
				System.out.println("#### table:" + tableName.toString() + " already exists!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			admin.close();// 记得最后关闭链接
		}

	}

	@Test
	@SuppressWarnings("deprecation")
	public void deleteTable() throws MasterNotRunningException, ZooKeeperConnectionException, Exception {
		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		TableName tableName = TableName.valueOf("phone_info");
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);// 先disable才能删除
				admin.deleteTable(tableName);
				System.out.println("#### table:" + tableName.toString() + " delete successful!");
			} else {
				System.out.println("table:" + tableName.toString() + " doesn't exists!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			admin.close();
		}
	}

	/**
	 * 向hbase中增加数据
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings({ "deprecation", "resource" })
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(534534534);
		// rowkey: phonenumber_timestamp
		ArrayList<Put> arrayList = new ArrayList<Put>();
		for (int i = 20181201; i < 20181231; i++) {
			Put put = new Put(Bytes.toBytes("18178656153_" + i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes((i % 2 + "")));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(i + ""));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("tophone"), Bytes.toBytes("17758651234"));
			arrayList.add(put);
		}

		try {
			// 插入数据
			table.put(arrayList);
			// 提交
			table.flushCommits();
			System.out.println("#### table:" + table.getName() + " insert successful!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 修改数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void updateData() throws Exception {
		Put put = new Put(Bytes.toBytes("18178656153_20181201"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("tophone"), Bytes.toBytes("17758651235"));
		// 插入数据
		table.put(put);
		// 提交
		table.flushCommits();
	}

	/**
	 * 删除数据:通过rowkey删除一整行数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteRowKey() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("18178656153_20181201"));
		table.delete(delete);
		table.flushCommits();
	}

	/**
	 * 删除数据:通过rowkey和列族删除指定行的某一列族所有数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteColumnFamily() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("18178656153_20181201"));
		delete.addFamily(Bytes.toBytes("info2"));
		table.delete(delete);
		table.flushCommits();
	}

	/**
	 * 删除数据:通过rowkey和列族和两名删除指定行的某一列族指定列数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteColumn() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("1234"));
		delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		table.delete(delete);
		table.flushCommits();
	}

	/**
	 * 单条查询
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryData() throws Exception {
		// 如果没有addFamily/addColumn就是查询所有列
		Get get = new Get(Bytes.toBytes("18178656153_20181201"));
		// get.addFamily(Bytes.toBytes("info"));
		// get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("type"));
		// get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"));
		// get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tophone"));
		Result result = table.get(get);
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("type"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("time"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("tophone"))));
	}

	/**
	 * 全表扫描:Range
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes("info"));
		// scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		scan.setStartRow(Bytes.toBytes("12341"));
		scan.setStopRow(Bytes.toBytes("12345"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}
	}

	/**
	 * 全表扫描的过滤器 :列值过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter1() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 过滤器：列值过滤器
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes("zhangsan2"));
		// 设置过滤器
		scan.setFilter(filter);

		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}

	}

	/**
	 * 全表扫描的过滤器 :rowkey过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter2() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 匹配rowkey以wangsenfeng开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^12341"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			// System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("password"))));
			// System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"),
			// Bytes.toBytes("name"))));
		}

	}

	/**
	 * 全表扫描的过滤器 :匹配列名前缀过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter3() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 匹配rowkey以wangsenfeng开头的
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("na"));
		// 设置过滤器
		scan.setFilter(filter);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println(
					"info:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"infi:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
				System.out.println(
						"info2:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info2:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"info2:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"))));
			}
		}

	}

	/**
	 * 全表扫描的过滤器 :过滤器集合
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDataByFilter4() throws Exception {

		// 创建全表扫描的scan
		Scan scan = new Scan();
		// 过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);
		// 匹配rowkey以wangsenfeng开头的
		RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^wangsenfeng"));
		// 匹配name的值等于wangsenfeng
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes("zhangsan"));
		filterList.addFilter(filter);
		filterList.addFilter(filter2);
		// 设置过滤器
		scan.setFilter(filterList);
		// 打印结果集
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowkey：" + Bytes.toString(result.getRow()));
			System.out.println(
					"info:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"infi:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
				System.out.println(
						"info2:name：" + Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
				System.out.println(
						"info2:age：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age"))));
			}
			// 判断取出来的值是否为空
			if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
				System.out.println(
						"info2:sex：" + Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex"))));
			}
		}

	}

	@After
	public void close() throws Exception {
		table.close();
		connection.close();
	}

}