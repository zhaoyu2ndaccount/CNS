package mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.examples.AppRequest;

/**
 * @author gaozy
 *
 */
public class MongoAppCleanupClient {
	private static Random rand = new Random();
	
	private static double fraction = 0.1;
	
	private final static int max_val = MongoApp.MAX_VALUE;
	
	// 10 attributes
	private final static int num_attributes = MongoApp.num_attributes;
	// prefix
	private final static String ATTR_PREFIX = MongoApp.ATTR_PREFIX;
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, JSONException{
		
		boolean toTest = false;
		if(args.length > 0)
			toTest = Boolean.parseBoolean(args[0]);
		
		if(System.getProperty("frac")!=null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
		MongoAppClient client = new MongoAppClient();
		
		if(toTest){
			MongoAppClient.createAllGroups(client, false);
			
			List<String> attributes = new ArrayList<String>();
			for (int k=0; k<num_attributes; k++) {
				attributes.add(ATTR_PREFIX+k);
			}
			
			List<String> givenList = new ArrayList<String>(attributes);
			Collections.shuffle(givenList);
			
			int num_selected_attr = 1; // rand.nextInt(num_attributes)+1;
			List<String> attr_selected = givenList.subList(0, num_selected_attr);
			
			// int drift = (int) (2*Math.round(max_val*Math.pow(fraction, 1.0/num_selected_attr)));
			double drift = max_val*Math.pow(fraction, 1.0/num_selected_attr);
//			System.out.println("Drift:"+drift);
			
			BasicDBObject query = new BasicDBObject();
			for (String attr: attr_selected) {
				BasicDBObject bson = new BasicDBObject();
				double start = rand.nextDouble() + rand.nextInt(max_val);
				double end = start + drift;
				bson.put("$gte", start);
				bson.put("$lt", end);
				query.put(attr, bson);
			}
			System.out.println("QUERY:"+query);
			
			Set<String> result = client.sendFindRequestAsync(query);
			System.out.println("Size of result(async) is:"+result.size());
			for (String str:result){
				System.out.println("Length:"+str.length());
				JSONArray arr = new JSONArray(str);
				int count = 0;
				for (int i = 0; i < arr.length(); i++) {
					System.out.println( arr.get(i) );
					count++;
				}
				System.out.println("Number of items:"+count);
			}
			
			result = client.sendFindRequest(query);
			System.out.println("Size of result(sync) is:"+result.size());
			for (String str:result){
				System.out.println("Length:"+str.length());
				// System.out.println(str);
			}
			
		} else {
						
			Document doc = new Document();
			JSONObject json = MongoAppClient.dropRequest(doc);
			Request response = client
					.sendRequest(new AppRequest(PaxosConfig.getDefaultServiceName(), json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false));
			System.out.println("Response:"+response);
		}
		
		client.close();
	}
}
