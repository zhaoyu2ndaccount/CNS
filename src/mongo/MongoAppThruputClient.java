package mongo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.reconfiguration.examples.AppRequest;


/**
 * @author gaozy
 *
 */
public class MongoAppThruputClient {

    private final static int num_outstanding_request_per_client = 8192;
    
 	private static Random rand = new Random();
	
	private static double ratio = 0.0;
	
	private static double fraction = 0.1;
	
	// 10 attributes
	private final static int num_attributes = MongoApp.num_attributes;
	// prefix
	private final static String ATTR_PREFIX = MongoApp.ATTR_PREFIX;
	// max value of range of each attribute
	private final static int max_val = MongoApp.MAX_VALUE;
	// max batch number
	private final static int max_batch = MongoApp.MAX_BATCH_NUM;
	
	private static List<String> keys = new ArrayList<String>();
	
	private static List<String> attributes = new ArrayList<String>();
	
	private static int numReplica;
	private static int numPartition;
	
    private static int sent = 0;    
    synchronized static void incrSent() {
    	sent++;
    }

    private static int rcvd = 0;
    synchronized  static void incrRcvd() {
        rcvd++;
    }
   
    private static void init(MongoAppClient client) {
		if (System.getProperty("ratio") != null) {
			ratio = Double.parseDouble(System.getProperty("ratio"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
		String fileName = client.getKeyFilename();
		
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	keys.add(line);
		    }
		    br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (int k=0; k<num_attributes; k++) {
			attributes.add(ATTR_PREFIX+k);
		}
		
		numReplica = (System.getProperty("numReplica")!=null)?
				Integer.valueOf(System.getProperty("numReplica")) : 1;	
		numPartition = (System.getProperty("numPartition")!=null)?
				Integer.valueOf(System.getProperty("numPartition")): 1;
	}
    
    private static class UpdateThread implements Runnable {
    	MongoAppClient client;
    	
		UpdateThread (MongoAppClient client) {
    		this.client = client;
    	}
    	
		@Override
		public void run() {
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
			incrSent();
			
			try {
				client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
						new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						synchronized (client) {
							incrRcvd();
							client.notify();
						}
					}
				});
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
    }
    
    private static class SearchThread implements Runnable {
    	MongoAppClient client;
    	
    	SearchThread(MongoAppClient client) {
    		this.client = client;
    	}

		@Override
		public void run() {
			List<String> givenList = new ArrayList<String>(attributes);
			Collections.shuffle(givenList);
			
			// select only one attribute
			int num_selected_attr = rand.nextInt(num_attributes)+1;
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
			
			// Request packet
			JSONObject reqVal = MongoAppClient.findRequest(query, max_batch);
			
			// Send to all partitions in a replica
			int idx = rand.nextInt(numReplica);
			for (int i=0; i<numPartition; i++){
				String serviceName = MongoAppClient.allServiceNames.get(i);
				List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
				InetSocketAddress addr = l.get(idx);
				AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
				request.setNeedsCoordination(false);
				
				incrSent();
				try {
					client.sendRequest(request, addr,
							new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							synchronized (client) {
								incrRcvd();
								client.notify();
							}
						}
					});
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
    	
    }
    
    
    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, InterruptedException {
    	MongoAppClient<String> client = new MongoAppClient<String>();
		
		// Create all service names
		MongoAppClient.createAllGroups(client, false);		
		// Thread.sleep(2000);
		
		init(client);
		
    	ExecutorService executor = Executors.newFixedThreadPool(1);

    	long begin = System.currentTimeMillis();
		long stop = begin + MongoApp.EXP_TIME;
    	while ( System.currentTimeMillis() < stop ) {
    		if(rand.nextDouble() < ratio){
    			executor.execute(new UpdateThread(client));
    		} else {
    			executor.execute(new SearchThread(client));
    		}
    		
    		while(sent - rcvd < num_outstanding_request_per_client){
    			synchronized(client){
    				client.wait(1000);
    			}
    		}
    	}
    	
    	System.out.println("Elapsed: "+(System.currentTimeMillis()-begin)/1000.0+"");
		System.out.println("Thruput: "+rcvd*1000.0/(MongoApp.EXP_TIME));
		
		client.close();
    }
}
