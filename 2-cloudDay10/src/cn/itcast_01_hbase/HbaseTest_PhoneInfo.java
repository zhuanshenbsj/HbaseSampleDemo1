package cn.itcast_01_hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest_PhoneInfo {
	Configuration conf = null;
	Connection conn = null;
	TableName tname = TableName.valueOf("phone_info");
	Log4JLogger log4jLogger = new Log4JLogger("HbaseTest_PhoneInfo");
	Random r = new Random();
	Table table;

	@Before
	public void init() throws IOException {
		conf = HBaseConfiguration.create();
		//conf.set(name, value);
		conn = ConnectionFactory.createConnection(conf);
		table = conn.getTable(tname);
	}

	/**
	 * 创建表
	 */
	@Test
	public void createTbl() {
		HBaseAdmin hAdmin = null;
		try {
			hAdmin = (HBaseAdmin) conn.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tname);
		HColumnDescriptor hCdesc = new HColumnDescriptor("info");
		hCdesc.setBlockCacheEnabled(true);
		hCdesc.setInMemory(true);
		hCdesc.setMaxVersions(1);
		hCdesc.setTimeToLive(86400);
		hTableDescriptor.addFamily(hCdesc);

		HColumnDescriptor hCdesc1 = new HColumnDescriptor("info1");
		hTableDescriptor.addFamily(hCdesc1);
		try {
			if (!hAdmin.tableExists(tname)) {
				hAdmin.createTable(hTableDescriptor);
				log4jLogger.info("####create table:" + tname.toString() + " successful!");
				return;
			}
			log4jLogger.info("#### table already exists!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				hAdmin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 删除表
	 */
	@Test
	public void deleteTbl() {
		HBaseAdmin hadmin = null;
		try {
			hadmin = (HBaseAdmin) conn.getAdmin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try {
			if (hadmin.tableExists(tname)) {
				hadmin.disableTable(tname);
				hadmin.deleteTable(tname);
				log4jLogger.info("####delete table:" + tname.toString() + " successful!");
			}
			log4jLogger.info("#### table:" + tname.toString() + " doesn't exists!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				hadmin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	/**
	 * 批量插入数据put(List)
	 * 插入十个手机号 100条通话记录 
	 * 满足查询 时间降序排序
	 */
	@Test
	public void insertPhoneInfo() {
		ArrayList<Put> arrayList = new ArrayList<Put>();
		for (int i = 0; i < 100; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 100; j++) {
				String phoneDateString = getDate("2016");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
				try {
					long dateLong = sdf.parse(phoneDateString).getTime();
					rowkey = phoneNum + "_" + (Long.MAX_VALUE - dateLong);
					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("type"), Bytes.toBytes((j % 2 + "")));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(phoneDateString.toString()));
					put.add(Bytes.toBytes("info"), Bytes.toBytes("tophone"), Bytes.toBytes(getPhoneNum("177")));
					arrayList.add(put);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		try {
			table.put(arrayList);
			log4jLogger.info("####put successsful!(" + arrayList.size() + ")");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 随机生成手机号
	 * @param prefix
	 * @return
	 */
	public String getPhoneNum(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}

	/**
	 * 随机生成年份
	 * @param year
	 * @return
	 */
	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d", new Object[] { r.nextInt(12) + 1, r.nextInt(30) + 1,
				r.nextInt(24) + 1, r.nextInt(60) + 1, r.nextInt(60) + 1, });
	}

	/**
	 * 查询某个手机 号某个月份下的所有通话详单(scan: startrow~stoprow)
	 * @throws ParseException 
	 */
	@Test
	public void scanPhoneMonthly() throws ParseException {
		String phonenum = "18601179481";//选一个手机号
		int m = r.nextInt(12);//随机一个月份
		System.out.println("###" + m + "- month message###");
		String start_month = String.format("%02d", (m + 1));
		String end_month = String.format("%02d", m);
		Scan scan = new Scan();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		String starRowKey = phonenum + "_" + (Long.MAX_VALUE - sdf.parse("2016" + start_month + "01000000").getTime());
		scan.setStartRow(starRowKey.getBytes());
		String stopRowKey = phonenum + "_" + (Long.MAX_VALUE - sdf.parse("2016" + end_month + "01000000").getTime());
		scan.setStopRow(stopRowKey.getBytes());
		try {
			ResultScanner rss = table.getScanner(scan);
			int count = 0;
			//遍历scan结果
			for (Result result : rss) {
				count++;
				System.out.println("--------------------------------------------------------------");
				System.out.println("rowkey-" + new String(result.getRow(), "utf-8"));
				System.out
						.println("info:type-" + Bytes.toString(result.getValue("info".getBytes(), "type".getBytes())));
				System.out
						.println("info:time-" + Bytes.toString(result.getValue("info".getBytes(), "time".getBytes())));
				System.out.println(
						"info:tophone-" + Bytes.toString(result.getValue("info".getBytes(), "tophone".getBytes())));

				Cell[] rawCells = result.rawCells();
				for (Cell cell : rawCells) {
					System.out.println("family=>" + new String(CellUtil.cloneFamily(cell), "utf-8") + "  value=>"
							+ new String(CellUtil.cloneValue(cell), "utf-8") + "  qualifer=>"
							+ new String(CellUtil.cloneQualifier(cell), "utf-8") + "  timestamp=>"
							+ cell.getTimestamp());
				}
			}
			log4jLogger.info("###total:" + count);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 查询某个手机号 所有主叫type=0的通话详单
	 * scan(filterList)
	 */
	@Test
	public void scanbyPhoneandType() {
		Filter tFilter = null;
		FilterList flist = new FilterList(Operator.MUST_PASS_ALL);
		String phonenum = "18601179481";//选一个手机号
		PrefixFilter prefixFilter = new PrefixFilter("18601179481".getBytes());
		flist.addFilter(prefixFilter);
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("info".getBytes(),
				"type".getBytes(), CompareOp.EQUAL, "0".getBytes());
		flist.addFilter(singleColumnValueFilter);
		Scan scan = new Scan();
		scan.setFilter(flist);
		try {
			ResultScanner scanner = table.getScanner(scan);
			int count = 0;
			for (Result result : scanner) {
				count++;
				System.out.println("--------------------------------------------------------------");
				System.out.println("rowkey-" + new String(result.getRow(), "utf-8"));
				for (Cell cell : result.listCells()) {
					System.out.println("Family-" + new String(CellUtil.cloneFamily(cell), "utf-8") + "\r\n"
							+ "Qualifier-" + new String(CellUtil.cloneQualifier(cell), "utf-8") + "\r\n" + "Value-"
							+ new String(CellUtil.cloneValue(cell), "utf-8") + "\r\n" + "timestamp-"
							+ cell.getTimestamp());
				}

			}
			log4jLogger.info("###total:" + count);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void end() {
		try {
			conn.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
