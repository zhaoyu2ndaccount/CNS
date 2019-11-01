package mongo;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author gaozy
 *
 */
public class MongoAppDimensionalitySetupClient {
	// 100K 
	private final static int num_records = MongoApp.num_records;
	// default 12 attributes
	private static int num_attributes = MongoApp.num_attributes;
	// prefix
	private final static String ATTR_PREFIX = MongoApp.ATTR_PREFIX;

	// max value of range of each attribute
	private final static int max_val = MongoApp.MAX_VALUE;

	protected final static int interval = max_val / 4;
	
	private final static Random rand = new Random();
	
	// generate actual subset by index sequence
	static int[] getSubset(int[] input, int[] subset) {
	    int[] result = new int[subset.length]; 
	    for (int i = 0; i < subset.length; i++) 
	        result[i] = input[subset[i]];
	    return result;
	}


	/**
	 * Use MongoAppClient to set up the whole system.
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, JSONException {

		HashFunction hf = Hashing.md5();

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

		if(System.getProperty("numAttr") != null) {
			num_attributes = Integer.parseInt(System.getProperty("numAttr"));
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
			List<String> partitions = new ArrayList<>();
			String key = MongoAppClient.getRandomKey(key_length);
			Document bson = new Document();
			bson.put(MongoApp.KEYS.KEY.toString(), key);
			for (int k=0; k<num_attributes; k++) {
				// FIXME: the range here is 0.0 to 100000.0
				// bson.put(ATTR_PREFIX+k, rand.nextInt(max_val)+rand.nextDouble());
				// FIXME: as requested by Ahmad, change the value to integer
				// bson.put(ATTR_PREFIX+k, rand.nextInt(10000000));
				// FIXME: generate actual value
				int v = rand.nextInt(Integer.MAX_VALUE);
				bson.put(ATTR_PREFIX+k, v);
				partitions.add(Integer.toString(v/interval));
			}

			String id = String.join("-", partitions);

			int idx = Hashing.consistentHash(hf.hashBytes(id.getBytes()), numPartition);

			MongoCollection<Document> coll = collections.get(idx);
			coll.insertOne(bson);
			if (num_req % 100000 == 0){
				System.out.println("It takes "+(System.currentTimeMillis()-last)/1000+" secs: "+num_req+" ...");
				last = System.currentTimeMillis();
			}
			num_req++;
			bw.write(key+"\n");

		}
				
		bw.close();
		fw.close();

		// Setup index
		Document all = new Document();
		JSONArray json_arr = new JSONArray();
		for (int i=0; i<num_attributes; i++){
			Document doc = new Document();
			// incremental order
			// doc.put(ATTR_PREFIX+i, 1);
			all.put(ATTR_PREFIX+i, 1);
			// json_arr.put(doc.toJson());
		}
		
		// Document doc = new Document();
		// doc.put(MongoApp.KEYS.KEY.toString(), 1);
		// json_arr.put(doc);
		json_arr.put(all);
		
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

		client.close();
	}
}
