package trumanz.hdfsTest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsWrapper {
	static void test(final String host) throws IOException{
		
		if(true) return;
		String response = null;
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic("admin", "admin");
		ClientConfig clientConfig = new ClientConfig().register(auth);
	    Client c = ClientBuilder.newClient(clientConfig);
        WebTarget target = c.target("http://172.17.0.1:8080");
        
        response  = target.path("/api/v1/clusters").request().get(String.class);
        //System.out.println(response);
       
        JsonParser jParser = new JsonParser();
        
        String clusterName = jParser.parse(response).getAsJsonObject()
        		.getAsJsonArray("items")
        		.get(0).getAsJsonObject().getAsJsonObject("Clusters")
        		.getAsJsonPrimitive("cluster_name").toString().replaceAll("\"", "");
       
        
       System.out.println("Cluster: " + clusterName);
       
       String path = "/api/v1/clusters/" + clusterName + "/services/HDFS/components/NAMENODE";
       response = target.path(path).request().get(String.class);
       
       //System.out.println(response);
       
       path = "/api/v1/clusters/" + clusterName + "/configurations";
       response = target.path(path).request().get(String.class);
       System.out.println(response);
       
       JsonArray jarray = jParser.parse(response).getAsJsonObject().getAsJsonArray("items");
       for(int i =0 ; i < jarray.size(); i++){
    	   String val = jarray.get(i).getAsJsonObject().getAsJsonPrimitive("type")
    			   .toString().replaceAll("\"", "");
    	   if(val.equals("core-site")){
    		   System.out.println(val);
    		   val = jarray.get(i).getAsJsonObject().getAsJsonPrimitive("href")
    				   .toString().replaceAll("\"", "");
    		   System.out.println(val);
    		   val = c.target("http://172.17.0.1:8080")
    		   		.path("/api/v1/clusters/test/configurations?type=hdfs-site&tag=version1")
    		   		.request().get(String.class);
    		  
    		   System.out.println(val);
    	   }
       }
       
       Path p = new Path("hdfs://172.17.0.2:8020/input");
       FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
       
       fs.listFiles(p, true);
       
	}
	
	

}
