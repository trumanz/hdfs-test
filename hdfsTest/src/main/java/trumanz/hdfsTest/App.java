package trumanz.hdfsTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	/*	
    	try{
    		
    	
    	Logger.getLogger("trumanz").info("Hello World");
    	
        AmbariWrapper ambari = new AmbariWrapper("http://as:8080", "test", "admin", "admin");
        
        String  hdfs_uri = ambari.getDefaultFS();
        //System.out.println(hdfs_uri);
       HdfsWrapper hdfs = new HdfsWrapper(hdfs_uri);
        //StringBuilder strBuilder = new StringBuilder();
        //for(int i =0 ; i < 128; i++) strBuilder.append('a');
        
        //hdfs.append("/input/tt1", strBuilder.toString());
        
      
    
        ambari.stopDataNode("ag2");
        
        hdfs.showPath("/input");
        Logger.getLogger("trumanz").info(hdfs.getBlockInformation("/input/tt1"));
        
        
         ambari.listDataNode();
        
        
        
         String[] clusters = AmbariWrapper.listClusters("http://as:8080", "admin", "admin");
         Logger.getLogger("trumanz").info(clusters.toString());
        
     //   ambari.stopServiceComponent();
    	}catch(javax.ws.rs.BadRequestException e){
    		
    		
    		Logger.getLogger("trumanz").warn(e.getResponse().getEntity());
    		Logger.getLogger("trumanz").warn(e.getResponse().getMetadata().toString());
    		//throw e;
    	}
    	*/
    }
}
