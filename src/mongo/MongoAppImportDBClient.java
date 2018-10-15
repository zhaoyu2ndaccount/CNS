package mongo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.reconfiguration.examples.AppRequest;

/**
 * @author gaozy
 *
 */
public class MongoAppImportDBClient {
	
	private static int rcvd = 0;
	synchronized static void incrRcvd() {
		rcvd++;
	}
	
	/**
	 * Use MongoAppClient to set up the whole system.
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
        
		MongoAppClient client = new MongoAppClient();

		int numReplica = 1;
		if(System.getProperty("numReplica") != null){
			numReplica = Integer.parseInt(System.getProperty("numReplica"));
		}
		int numPartition = 1;
		if(System.getProperty("numPartition") != null) {
			numPartition = Integer.parseInt((System.getProperty("numPartition")));
		}
		
		// Create all service names
		MongoAppClient.createAllGroups(client, true);
		
		// It takes a few seconds to initialize all groups
		Thread.sleep(numReplica*2000);
		
		int total = numReplica* numPartition;
		
		for (int i=0; i<numPartition; i++) {
			//Command: mongorestore -d active -c AR0 /proj/anolis-PG0/groups/1-node/active/AR0.bson
			// FIXME: this only works for consistent hash scheme, figure out what to add			
			List<InetSocketAddress> group = MongoAppClient.getGroupForAllServiceNames().get(i);
			String serviceName = MongoAppClient.allServiceNames.get(i);
			String path = "/proj/anolis-PG0/groups/"+numPartition+"-partition/active/AR"+i+".bson";
			JSONObject json = MongoAppClient.restoreRequest(path);			
			
			for (InetSocketAddress addr:group) {
				AppRequest request = new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
				request.setNeedsCoordination(false);
				
				// send request to the service name
				client.sendRequest(request, addr,
						new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						System.out.println("Received response from ["+addr+"].");
						synchronized (client) {
							incrRcvd();
						}
					}
				});	
			}
		}		
		
		while (rcvd < total){
			Thread.sleep(500);
		}
		
		// create key index for update
		JSONArray json_arr = new JSONArray();
		Document doc = new Document();
		doc.put(MongoApp.KEYS.KEY.toString(), 1);
		json_arr.put(doc.toJson());
		JSONObject json = MongoAppClient.indexRequest(json_arr);
		for (int i=0; i<numPartition; i++) {
			String serviceName = MongoAppClient.allServiceNames.get(i);
			AppRequest request = new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
			client.sendRequest(request);
		}
		
		client.close();
	}
}
