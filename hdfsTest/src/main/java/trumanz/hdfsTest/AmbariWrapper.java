package trumanz.hdfsTest;

import java.net.MalformedURLException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import com.google.gson.JsonParser;

public class AmbariWrapper {
	
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
	private	WebTarget wtarget;
	private JsonParser jParser;

}
