package trumanz.hdfsTest;

import java.io.FileNotFoundException;
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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;


public class HdfsWrapper {
	public HdfsWrapper(String base_uri) throws IllegalArgumentException, IOException, InterruptedException{
		this.base_path = new Path(base_uri);
		this.fs = FileSystem.get(base_path.toUri(), new Configuration(), "hdfs");
	}
	public void showPath(final String path) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		RemoteIterator<LocatedFileStatus> filestates = fs.listFiles(new Path(base_path, path), true);
		while(filestates.hasNext()){
			LocatedFileStatus fstat  = filestates.next();
			System.out.println(fstat.toString());
		}
	}
	public void append(final String path, final String content) throws IOException{
		Path p = new Path(base_path, path);
		FSDataOutputStream outStream = fs.create(p);
		
		outStream.write(content.getBytes());
		outStream.close();
	}
	
	public String getBlockInformation(final String path) throws IOException{
		StringBuilder strBuilder = new StringBuilder();
		Path p = new Path(base_path, path);
		FileStatus fstatus = fs.getFileStatus(p);
		BlockLocation[] blocs = fs.getFileBlockLocations(p, 0, fstatus.getLen());
		int i = 0;
		strBuilder.append(p.toString() + " Len=" + fstatus.getLen() + ",blockSize=" + fstatus.getBlockSize());
		for(BlockLocation bloc : blocs){
			strBuilder.append("\n block" + i++  + " on host: ");
			for(String host : bloc.getHosts()){
				strBuilder.append(host + ", ");
			}
		}
		
		return strBuilder.toString();
	}
	
	private FileSystem fs;
	private Path base_path;

}
