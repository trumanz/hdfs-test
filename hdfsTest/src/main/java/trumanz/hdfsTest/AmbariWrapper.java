package trumanz.hdfsTest;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.client.ClientResponse;


public class AmbariWrapper {
	private Client  client;
	private	WebTarget wtarget;
	private JsonParser jParser;
	public AmbariWrapper(final String target, final String user, final String passwd){
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(user, passwd);
		ClientConfig clientConfig = new ClientConfig().register(auth);
		client = ClientBuilder.newClient(clientConfig);
		wtarget = client.target(target);
		jParser = new JsonParser();
		
	}
	
	public String getDefaultFS() {
		
        String val  = wtarget.path("/api/v1/clusters/test/configurations")
        		.queryParam("type", "core-site")
        		.queryParam("tag", "version1")
        		.request().get(String.class);
        //System.out.println(val);
        val = jParser.parse(val).getAsJsonObject()
        		.getAsJsonArray("items").get(0).getAsJsonObject()
        		.getAsJsonObject("properties")
        		.getAsJsonPrimitive("fs.defaultFS").toString().replaceAll("\"", "");
        return val;
	}
	
	public boolean stopDataNode(String host) throws InterruptedException{
		
		String json = CreateRequestJson("STOP DATANODE", "{\"HostRoles\":{\"state\" : \"INSTALLED\"}}");
	
		//APPLICATION_FORM_URLENCODED, Do no use JSON!!! 
		Entity<String> entity = Entity.entity(json, MediaType.APPLICATION_FORM_URLENCODED);
		
		String val =  wtarget.path("/api/v1/clusters/test/hosts/ag2/host_components/DATANODE")
			.request().put(entity, String.class);
	
		Logger.getLogger("trumanz").debug(val);
		
		val = jParser.parse(val).getAsJsonObject()
				.getAsJsonPrimitive("href").toString().replace("\"","");
		
		Logger.getLogger("trumanz").info("mointing reqeust status on:" + val);
		
		while(true){
			String status = client.target(val).request().get(String.class);
			status = jParser.parse(status).getAsJsonObject()
					.getAsJsonObject("Requests")
					.getAsJsonPrimitive("progress_percent").toString();
			if(status.equals("100.0")){
				break;
			} else {
				Logger.getLogger("trumanz").info("progress_percent:" + status);
				Thread.sleep(5*1000);
			}
		}
		
		Logger.getLogger("trumanz").info(val);
		
		return true;
	}
	
	public String CreateRequestJson(String reqContext, String body){
		//just try uing GSON
		Gson gson = new Gson();
		Logger.getLogger("trumanz").info(body);
		
		Map<String, String> kv = new LinkedHashMap<String, String>();
		Map<String, Object> json = new LinkedHashMap<String,Object>();
		
		
		kv.put("context", reqContext);
		//kv.put("query", "null");

		json.put("RequestInfo", kv);
		
		json.put("Body", gson.fromJson(body, JsonObject.class));
	
		String val = gson.toJsonTree(json).toString();
		
		
		Logger.getLogger("trumanz").debug(val);
		return val;
		
	}
	

}
