package mongo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;

import edu.umass.cs.gigapaxos.PaxosConfig;

/**
 * @author gaozy
 *
 */
public class MongoAppImportDBClient {
	
	private final static int num_attributes = MongoApp.num_attributes;
	
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
		
		HashMap<Integer, MongoCollection<Document>> allCollections = new HashMap<Integer, MongoCollection<Document>>();
		Map<String, InetSocketAddress>actives = PaxosConfig.getActives();
		int port = 27017;
		int id = 0;
		for (String name: actives.keySet()) {
			String table_name = name;
			String host = actives.get(name).getHostName(); // ((List<InetSocketAddress>) allGroups.get(id)).getHostName();
			
			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient(host, port);
			MongoDatabase database = mongoClient.getDatabase(MongoDBServiceApp.DB_NAME);
			MongoCollection<Document> collection = database.getCollection(table_name);
			
			allCollections.put(id, collection);
			++id;
		}
		
		// Create all service names
		MongoAppClient.createAllGroups(client, true);
		
		// It takes a few seconds to initialize all groups
		Thread.sleep(numReplica*2000);
		
		// int total = numReplica* numPartition;
		//TODO: restore DB out of the code
		/*
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
		*/
		
		List<Thread> th_pool = new ArrayList<Thread>();
		
		// create key index for update
		
		List<IndexModel> indexes = new ArrayList<IndexModel>();
		for (int i=0; i<num_attributes; i++) {
			BasicDBObject obj = new BasicDBObject();
			obj.append(MongoApp.ATTR_PREFIX+i, 1);
			IndexModel model = new IndexModel(obj);
			indexes.add(model);
		}
		for (int i=0; i<numPartition; i++) {
			final MongoCollection<Document> coll = allCollections.get(i);
			Thread th = new Thread(new Runnable(){
				@Override
				public void run() {
					coll.createIndexes(indexes);
				}
			});
			th_pool.add(th);
			th.start();
		}
		
		for (Thread th:th_pool){
			th.join();
		}
		
		client.close();
	}
}
