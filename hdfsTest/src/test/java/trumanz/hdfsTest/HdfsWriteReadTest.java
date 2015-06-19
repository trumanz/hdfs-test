package trumanz.hdfsTest;

//import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import  org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HdfsWriteReadTest {
	private static FileSystem fs;
	private static String basePath  = "/tmp/test/";
	private static AmbariWrapper ambari;
	private Logger logger  = Logger.getLogger("trumanz");
	@BeforeClass
	public static  void setUp() throws Exception{
		ambari = AmbariWrapper.getAsFirstCluster("http://as:8080",  "admin", "admin");
		String hdfs_uri = ambari.getDefaultFS();
		fs = FileSystem.get(new URI(hdfs_uri), new Configuration(), "hdfs");
		fs.delete(new Path(basePath), true);
		fs.mkdirs(new Path(basePath));
	}
	
	@AfterClass
	public static void tearDown() throws IOException{
		
		fs.close();
	}
	
	
	@Test
	public void test1CreateReadDelete() throws Exception{
		Path path = new Path(basePath + "test.file");
		FSDataOutputStream fout = fs.create(path);
		fout.write(new String("abcde").getBytes());
		fout.close();
		FSDataInputStream fin = fs.open(path);
		byte[] bytes = new byte[100];
		int len = fin.read(bytes);
		Assert.assertEquals(len, 5);
		fs.delete(path,false);
		Assert.assertFalse(fs.exists(path));
	}
	
	@Test
	public void test2DataNodeDownTest() throws Exception{
		Path path = new Path(basePath + "/DataNodeDownTest/test.file");
		//1. Start All Data Node, and not less than 3 Data Nodes;
		List<String> dataNodeHosts  = ambari.listHostByComponent("DATANODE");
		Assert.assertTrue(dataNodeHosts.size() >=3);
		for(String node : dataNodeHosts){
			ambari.startComponent(node, "DATANODE");
		}
		//2. Create a file with 2 replications;
		short replication = 2;
		FSDataOutputStream fout = fs.create(path, replication);
		fout.writeChars("www.baidu.com");
		fout.close();
		String[]  blockHosts = fs.getFileBlockLocations(fs.getFileStatus(path), 0, 1)[0].getHosts();
		Assert.assertTrue(blockHosts.length == 2);
		logger.info("blockHosts :" + Arrays.toString(blockHosts));
		//3. Stop one DataNode with that hit the replication and recheck the block host
		ambari.stopComponent(blockHosts[0], "DATANODE");
		blockHosts = fs.getFileBlockLocations(fs.getFileStatus(path), 0, 1)[0].getHosts();
		logger.info("blockHosts :" + Arrays.toString(blockHosts));
	}

}
