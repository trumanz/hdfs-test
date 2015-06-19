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
	static Logger logger  = Logger.getLogger("trumanz");
	
	static String[] listClusters(final String ambariServer, final String user, String passwd){
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(user, passwd);
		ClientConfig clientConfig = new ClientConfig().register(auth);
		Client  client = ClientBuilder.newClient(clientConfig);
		String resp = client.target(ambariServer + "/api/v1/clusters").request().get(String.class);
		logger.debug(resp);
		JsonParser jParser = new JsonParser();
		JsonArray jarray = jParser.parse(resp).getAsJsonObject().getAsJsonArray("items");
		if(jarray.size() == 0) return null;
		String[] clusters = new String[jarray.size()];
		for(int i =0; i < jarray.size(); i++){
			 String cluster = jarray.get(i).getAsJsonObject()
					 .getAsJsonObject("Clusters").getAsJsonPrimitive("cluster_name")
					 .toString().replace("\"", "");
			 clusters[i] = cluster;
		}
		return clusters;
	}
	
	static AmbariWrapper getAsFirstCluster(final String ambariServer, 
			final String user, String passwd) throws Exception{
		
		String[] cluster = AmbariWrapper.listClusters(ambariServer, user, passwd);
		if(cluster.length == 0){
			throw new Exception("There were no cluster");
		}
		logger.info("There were " + cluster.length + " cluster, now use " + cluster[0]);
		return new  AmbariWrapper(ambariServer, cluster[0], user, passwd);
	}
	
	
	
	private Client  client;
	private	WebTarget clusterTarget;
	private JsonParser jParser = new JsonParser();
	private Gson gson = new Gson();
	private List<HostRole>  hostRoles;
	
	public AmbariWrapper(final String ambariServer, final String cluster, 
			final String user, final String passwd){
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(user, passwd);
		ClientConfig clientConfig = new ClientConfig().register(auth);
		client = ClientBuilder.newClient(clientConfig);
		clusterTarget = client.target(ambariServer).path("/api/v1/clusters/" + cluster);
		hostRoles  = this.listHostComponents(this.listHost());
	}
	
	public String getDefaultFS() {
		
        String val  = clusterTarget.path("/configurations")
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
	
	public  List<String> listHostByComponent(String compName){
		List<String> hosts = new LinkedList<String>();
		for(HostRole role :  hostRoles)
		{
			if( role.component_name.equals(compName)){
				hosts.add(role.host_name);
			}	
		}
		return hosts;
		
	}
	
	public String[] listDataNode(){
		 String val  = clusterTarget.path("/services/HDFS/components/DATANODE")
				 .request().get(String.class);
		 Logger.getLogger("trumanz").debug(val);
		 JsonArray jarray  = jParser.parse(val).getAsJsonObject()
				 .getAsJsonArray("host_components");
		 String[] hosts = new String[jarray.size()];
		 for(int i =0; i < jarray.size(); i++){
			 String host = jarray.get(i).getAsJsonObject().getAsJsonObject("HostRoles")
					 .getAsJsonPrimitive("host_name").toString().replace("\"", "");
			 hosts[i] = host;
		 }
		 return hosts;
	}
	
	public void startComponent(String host, String comp) throws InterruptedException{
		manageCompoent(host, comp, true);
	}
	public void stopComponent(String host, String comp) throws InterruptedException{
		manageCompoent(host, comp, false);
	}
	
	void manageCompoent(String host,String comp, boolean start) throws InterruptedException{
		
		String context = start ? "Start " : "Stop";
		context  = context + " " + comp +  " on " + host;
		String state =  start ? "STARTED" : "INSTALLED";
		String json = CreateRequestJson(context, "{\"HostRoles\":{\"state\" : \"" + state +  "\"}}");
	
		//APPLICATION_FORM_URLENCODED, Do no use JSON!!! 
		Entity<String> entity = Entity.entity(json, MediaType.APPLICATION_FORM_URLENCODED);
		
		Response resp =  clusterTarget.path("/hosts/" + host + "/host_components/" + comp)
			.request().put(entity, Response.class);
		Logger.getLogger("trumanz").debug(resp.getStatus());
		
		if(resp.getStatus() == 200) return;
		
		String respEntity = resp.readEntity(String.class);
		Logger.getLogger("trumanz").debug(respEntity);
		
		String val = jParser.parse(respEntity).getAsJsonObject()
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
				Logger.getLogger("trumanz").info(context + " progress_percent:" + status);
				Thread.sleep(5*1000);
			}
		}
		
		Logger.getLogger("trumanz").info(val);
		return;
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
	
	
	
	public void startAllDataNode() throws InterruptedException
	{
		for(HostRole role :  hostRoles)
		{
			if( role.component_name.equals("DATANODE")){
				logger.info("Try to start DATANODE on " + role.host_name);
				startComponent(role.host_name, "DATANODE");
			}	
		}
	}
	
	
	private String[] listHost(){
		String resp = clusterTarget.path("/hosts").request().get(String.class);
		JsonArray jarray = jParser.parse(resp).getAsJsonObject()
				.getAsJsonArray("items");
		if(jarray.size() == 0 ) return null;
		String[] hosts = new String[jarray.size()]; 
		for(int i = 0; i < jarray.size(); i++)
		{
			String h = jarray.get(i).getAsJsonObject().getAsJsonObject("Hosts")
			.getAsJsonPrimitive("host_name").toString().replace("\"", "");
			hosts[i] = h;
		}
		return hosts;
	}
	
	
	List<HostRole> listHostComponents(String[] hosts){
		List<HostRole>  hostRoles = new LinkedList<HostRole>();
		for(String host : hosts){
			String resp = clusterTarget.path("/hosts/" + host)
					.request().get(String.class);
			JsonArray jarray = jParser.parse(resp).getAsJsonObject()
					.getAsJsonArray("host_components");
			for(JsonElement jelem : jarray)
			{
				
				HostRole hostRole = gson.fromJson(
						jelem.getAsJsonObject().getAsJsonObject("HostRoles"), HostRole.class);
				hostRoles.add(hostRole);
			}
		}
		return hostRoles;
	}
	
	
	List<AmbariComponent>  listServiceComponents(String serviceName){
		List<AmbariComponent> comps = new LinkedList<AmbariComponent>();
		String resp = clusterTarget.path("/services/" + serviceName + "/components")
				.request().get(String.class);
		logger.debug(resp);
		
		JsonArray jarray = jParser.parse(resp).getAsJsonObject()
				.getAsJsonArray("items");
		for(JsonElement jelem : jarray)
		{	
			AmbariComponent comp = gson.fromJson(jelem, AmbariComponent.class);
			comps.add(comp);
		}
		return comps;
	}
	
	

}
