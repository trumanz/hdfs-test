package trumanz.hdfsTest;

//import static org.junit.Assert.assertEquals;
import  org.junit.Assert;
import org.junit.Test;

public class HdfsWriteReadTest {
	
	@Test
	public void Read() throws Exception{
		//Assert.assertEquals(message, expecteds, actuals);
		AmbariWrapper ambari = AmbariWrapper.getAsFirstCluster("http://as:8080",  "admin", "admin");
		String hdfs_uri = ambari.getDefaultFS();
		HdfsWrapper hdfs =  new HdfsWrapper(hdfs_uri);
		hdfs.createFile("/tmp/test.file", "This is test");
		
		System.out.println(hdfs.getBlockInformation("/tmp/test.file"));
	}

}
