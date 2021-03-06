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
import org.json.JSONException;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexModel;

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
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, JSONException {
		// List<int[]> result = genertateComb(num_attributes, 1);
		// System.exit(0);
        
		MongoAppClient client = new MongoAppClient();
		
		// Create all service names
		MongoAppClient.createAllGroups(client, false);
		
		int numReplica = 1;
		int numPartition = 1;
		if(System.getProperty("numReplica") != null){
			numReplica = Integer.parseInt(System.getProperty("numReplica"));
		}
		
		if(System.getProperty("numPartition") != null){
			numPartition = Integer.parseInt(System.getProperty("numPartition"));
		}
		
		
		Map<Integer, MongoCollection<Document>> collections = client.allCollections;
		for (int i:collections.keySet()) {
			System.out.println(i+":"+collections.get(i));
		}
		
		// It takes a few seconds to initialize all groups
		Thread.sleep(numReplica*2000);
		
		FileWriter fw = new FileWriter(MongoAppClient.getKeyFilename());
		BufferedWriter bw = new BufferedWriter(fw);
		int key_length = MongoAppClient.getKeyLength();
		
		int total = 0;
		if (System.getProperty("num_records") != null){
			total = Integer.parseInt(System.getProperty("num_records"));
		} else {
			total = num_records;
		}
		
		int num_req = 0;
		long last = System.currentTimeMillis();
		// sequentially insert all the records, impossible to "Too many outstanding requests" exception
		while ( num_req<total ) {
			String key = MongoAppClient.getRandomKey(key_length);
			Document bson = new Document();
			bson.put(MongoApp.KEYS.KEY.toString(), key);
			for (int k=0; k<num_attributes; k++) {
				// FIXME: the range here is 0.0 to 100000.0
				// bson.put(ATTR_PREFIX+k, rand.nextInt(max_val)+rand.nextDouble());
				// FIXME: as requested by Ahmad, change the value to integer
				bson.put(ATTR_PREFIX+k, rand.nextInt(10000000));
			}
			
			byte[] b = key.getBytes();
			int retval = b.hashCode();
			
			int idx = retval % numPartition;
			// System.out.println(num_req+":"+idx);
			MongoCollection<Document> coll = collections.get(idx);
			coll.insertOne(bson);
			if (num_req % 100000 == 0){
				System.out.println("It takes "+(System.currentTimeMillis()-last)/1000+" secs: "+num_req+" ...");
				last = System.currentTimeMillis();
			}
			num_req++;
			bw.write(key+"\n");
			/*
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
			*/
			
		}
				
		bw.close();
		fw.close();
		
		// Map<Integer, String> allServiceNames = MongoAppClient.allServiceNames;
		
		// Setup index
		// Document all = new Document();
		JSONArray json_arr = new JSONArray();
		for (int i=0; i<num_attributes; i++){
			Document doc = new Document();
			// incremental order
			doc.put(ATTR_PREFIX+i, 1);
			// all.put(ATTR_PREFIX+i, 1);
			json_arr.put(doc.toJson());
		}
		
		Document doc = new Document();
		doc.put(MongoApp.KEYS.KEY.toString(), 1);
		json_arr.put(doc);
		// json_arr.put(all);
		
		for (int k:collections.keySet()) {
			long begin = System.currentTimeMillis();
			MongoCollection<Document> coll = collections.get(k);
			List<IndexModel> list = new ArrayList<IndexModel>();
			for (int i=0; i<json_arr.length(); i++) {
				list.add(new IndexModel( Document.parse(json_arr.get(i).toString()) ) );
			}
			coll.createIndexes(list);
			long elapsed = System.currentTimeMillis() - begin;
			System.out.println("Setup index successfully "+k+":"+elapsed+"ms.");
		}
		/*
		for (int i=0; i<allServiceNames.keySet().size(); i++) {
			String serviceName = allServiceNames.get(i);
			JSONObject json = MongoAppClient.indexRequest(json_arr);
			Request response = client
					.sendRequest(new AppRequest(serviceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
			System.out.println("Setup index successfully "+i+":"+response);
		}*/
		
		client.close();
	}
}
