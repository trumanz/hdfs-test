package trumanz.hdfsTest;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	
    	Logger.getLogger("trumanz").info("Hello World");
    	
        AmbariWrapper ambari = new AmbariWrapper("http://as:8080", "admin", "admin");
        String  hdfs_uri = ambari.getDefaultFS();
        //System.out.println(hdfs_uri);
        HdfsWrapper hdfs = new HdfsWrapper(hdfs_uri);
        StringBuilder strBuilder = new StringBuilder();
        for(int i =0 ; i < 128; i++) strBuilder.append('a');
        
        hdfs.append("/input/tt1", strBuilder.toString());
        //hdfs.showPath("/input");
        Logger.getLogger("trumanz").info(hdfs.getBlockInformation("/input/tt1"));
        
        ambari.stopDataNode("ag2");
     //   ambari.stopServiceComponent();
    }
}
