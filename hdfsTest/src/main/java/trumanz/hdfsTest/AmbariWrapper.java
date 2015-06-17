package trumanz.hdfsTest;

import java.net.MalformedURLException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class AmbariWrapper {
	private	WebTarget wtarget;
	private JsonParser jParser;
	public AmbariWrapper(final String target, final String user, final String passwd){
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(user, passwd);
		ClientConfig clientConfig = new ClientConfig().register(auth);
		wtarget = ClientBuilder.newClient(clientConfig).target(target);
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
	
	public boolean stopDataNode(String host){
		
		JsonObject jobj1 = new JsonObject();
		JsonObject jobj2 = new JsonObject();
		jobj2.addProperty("state", "INSTALLED");
		jobj1.add("HostRoles",jobj2);
		
		Entity<String> entity = Entity.entity(jobj1.toString(), MediaType.APPLICATION_JSON);
		
		String resp =  wtarget.path("/api/v1/clusters/test/hosts/" + host + "/host_components/DATANODE")
			.request().put(entity, String.class);
			//.put(jobj1.toString());
		
		Logger.getLogger("trumanz").info(resp);
		
		return true;
	}
	

}
