package mongo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;

/**
 * @author gaozy
 *
 */
public class MongoAppSetupClient {
	// 100K 
	private final static int num_records = MongoApp.num_records;
	// 10 attributes
	private final static int num_attributes = MongoApp.num_attributes;
	// prefix
	private final static String ATTR_PREFIX = MongoApp.ATTR_PREFIX;
	// max value of range of each attribute
	private final static int max_val = MongoApp.MAX_VALUE;
	
	
	private final static Random rand = new Random();
	
	private static int sent = 0;
	synchronized static void incrSent() {
		sent++;
	}
	
	private static int rcvd = 0;
	synchronized static void incrRcvd() {
		rcvd++;
	}
	
	// generate actual subset by index sequence
	static int[] getSubset(int[] input, int[] subset) {
	    int[] result = new int[subset.length]; 
	    for (int i = 0; i < subset.length; i++) 
	        result[i] = input[subset[i]];
	    return result;
	}
	
	/**
	 * @param m : length of the array
	 * @param k : sequence length  
	 * @return a list of generated combination
	 */
	public static List<int[]> genertateComb(int m, int k) {
		int[] input = new int[m];    // input array
		for (int i=0; i<m; i++) {
			input[i] = i;
		}

		List<int[]> subsets = new ArrayList<>();

		int[] s = new int[k];                  // here we'll keep indices 
		                                       // pointing to elements in input array

		if (k <= input.length) {
		    // first index sequence: 0, 1, 2, ...
		    for (int i = 0; (s[i] = i) < k - 1; i++);  
		    subsets.add(getSubset(input, s));
		    for(;;) {
		        int i;
		        // find position of item that can be incremented
		        for (i = k - 1; i >= 0 && s[i] == input.length - k + i; i--); 
		        if (i < 0) {
		            break;
		        }
		        s[i]++;                    // increment this item
		        for (++i; i < k; i++) {    // fill up remaining items
		            s[i] = s[i - 1] + 1; 
		        }
		        subsets.add(getSubset(input, s));
		    }
		}
		
		return subsets;
	}
	
	/**
	 * Use MongoAppClient to set up the whole system.
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		// List<int[]> result = genertateComb(num_attributes, 1);
		// System.exit(0);
        
		MongoAppClient<String> client = new MongoAppClient<String>();
		
		// Create all service names
		MongoAppClient.createAllGroups(client, true);
		
		int numReplica = 1;
		if(System.getProperty("numReplica") != null){
			numReplica = Integer.parseInt(System.getProperty("numReplica"));
		}
		
		// It takes a few seconds to initialize all groups
		Thread.sleep(numReplica*2000);
		
		FileWriter fw = new FileWriter(client.getKeyFilename());
		BufferedWriter bw = new BufferedWriter(fw);
		int key_length = MongoAppClient.getKeyLength();
		
		int total = 0;
		if (System.getProperty("num_records") != null){
			total = Integer.parseInt(System.getProperty("num_records"));
		} else {
			total = num_records;
		}
		
		int num_req = 0;
		// sequentially insert all the records, impossible to "Too many outstanding requests" exception
		while ( num_req<total ) {
			String key = MongoAppClient.getRandomKey(key_length);
			Document bson = new Document();
			bson.put(MongoApp.KEYS.KEY.toString(), key);
			for (int k=0; k<num_attributes; k++) {
				bson.put(ATTR_PREFIX+k, rand.nextInt(max_val)+rand.nextDouble());
			}
			JSONObject json = MongoAppClient.insertRequest(bson);
			String serviceName = client.getServiceName(bson);
			
			incrSent();
			
			client.sendRequest(ReplicableClientRequest.wrap(new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false)),					
					new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						synchronized (client) {
							incrRcvd();
							client.notify();
						}
					}
			});	
			
			num_req++;
			/*
			Request response = client
					.sendRequest(new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
					*/
			
			bw.write(key+"\n");
			
			while ( sent - rcvd > 1000) {
				synchronized(client){
					client.wait(1000);
				}
			}
			
		}
				
		bw.close();
		fw.close();
		
		Map<Integer, String> allServiceNames = MongoAppClient.allServiceNames;
		
		// Setup index
		JSONArray json_arr = new JSONArray();
		for (int i=0; i<num_attributes; i++){	
			/* 
			List<int[]> result = genertateComb(num_attributes, i);
			for (int[] comb: result) {
				Document doc = new Document();
				for (int j=0; j<comb.length; j++) {
					// incremental order
					doc.put(ATTR_PREFIX+comb[j], 1);
				}
				json_arr.put(doc.toJson());
			}
			*/
			Document doc = new Document();
			// incremental order
			doc.put(ATTR_PREFIX+i, 1);
			json_arr.put(doc.toJson());
		}
		
		
		for (int i=0; i<allServiceNames.keySet().size(); i++) {
			String serviceName = allServiceNames.get(i);
			JSONObject json = MongoAppClient.indexRequest(json_arr);
			Request response = client
					.sendRequest(new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
			System.out.println("Setup index successfully "+i+":"+response);
		}
		
		client.close();
	}
}
