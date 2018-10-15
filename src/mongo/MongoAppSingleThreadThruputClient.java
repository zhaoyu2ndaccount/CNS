package mongo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.DelayProfiler;

/**
 * @author gaozy
 *
 */
public class MongoAppSingleThreadThruputClient {
	
	private static Random rand = new Random();
	
	private static double ratio = 0.0;
	
	private static double fraction = 0.1;
	
	// 10 attributes
	private final static int num_attributes = MongoApp.num_attributes;
	// prefix
	private final static String ATTR_PREFIX = MongoApp.ATTR_PREFIX;
	// max value of range of each attribute
	private final static int max_val = MongoApp.MAX_VALUE;
	
	
	private static void init() {
		if (System.getProperty("ratio") != null) {
			ratio = Double.parseDouble(System.getProperty("ratio"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
	}
	
	/**
	 * @MongoAppClient to stress test the system capacity
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		
		
		MongoAppClient client = new MongoAppClient();
		
		// Create all service names
		MongoAppClient.createAllGroups(client, false);		
		// Thread.sleep(2000);	
		
		init();
		
		String fileName = MongoAppClient.getKeyFilename();
		
		List<String> keys = new ArrayList<String>();
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	keys.add(line);
		    }
		    br.close();
		}
		
		
		List<String> attributes = new ArrayList<String>();
		for (int k=0; k<num_attributes; k++) {
			attributes.add(ATTR_PREFIX+k);
		}
		
		int count = 0;
		long begin = System.currentTimeMillis();
		long stop = begin + MongoApp.EXP_TIME;
		while (System.currentTimeMillis() < stop) {
			long t1 = System.currentTimeMillis();
			// keep sending back-to-back request
			if(rand.nextDouble() < ratio){
				String key = keys.get(rand.nextInt(keys.size()));
				Document oldVal = new Document();
				oldVal.put(MongoApp.KEYS.KEY.toString(), key);
				Document newVal = new Document();
				newVal.put(MongoApp.KEYS.KEY.toString(), key);
				for (int k=0; k<num_attributes; k++) {
					newVal.put(ATTR_PREFIX+k, rand.nextInt());
				}
				
				JSONObject req = MongoAppClient.replaceRequest(oldVal, newVal);
				
				String serviceName = client.getServiceName(oldVal);
				client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
							
			} else {
				List<String> givenList = new ArrayList<String>(attributes);
				Collections.shuffle(givenList);
				
				// select only one attribute
				int num_selected_attr = 1; // rand.nextInt(num_attributes)+1;
				List<String> attr_selected = givenList.subList(0, num_selected_attr);
				
				// int drift = (int) (2*Math.round(max_val*Math.pow(fraction, 1.0/num_selected_attr)));
				double drift = max_val*Math.pow(fraction, 1.0/num_selected_attr);
//				System.out.println("Drift:"+drift);
				
				BasicDBObject query = new BasicDBObject();
				for (String attr: attr_selected) {
					BasicDBObject bson = new BasicDBObject();
					double start = rand.nextDouble() + rand.nextInt(max_val);
					double end = start + drift;
					bson.put("$gte", start);
					bson.put("$lt", end);
					query.put(attr, bson);
				}
				// System.out.println("QUERY:"+query);
				
				
				// Async search is more efficient
				client.sendFindRequestAsync(query);
				// client.sendFindRequest(query);
			}
			count++;
			DelayProfiler.updateDelay("search_or_update", t1);
		}
		
		System.out.println(DelayProfiler.getStats());
		System.out.println("Elapsed: "+(System.currentTimeMillis()-begin)/1000.0+"");
		System.out.println("Thruput: "+count*1000.0/(MongoApp.EXP_TIME));
		
		client.close();
	}
}
