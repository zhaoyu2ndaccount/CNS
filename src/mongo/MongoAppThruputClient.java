package mongo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.DelayProfiler;


/**
 * @author gaozy
 *
 */
public class MongoAppThruputClient {

 	private static Random rand = new Random();

	private static double ratio = 0.0;

	private static double fraction = 0.00001;

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
	
	private static int num_clients = 10;
	
	private static int numReplica;
	private static int numPartition;
    
    // private static int num_updates = 0;
    private static int req_id = 0;
    synchronized static int getReqId() {
    	return req_id++;
    }
    
    private static ConcurrentHashMap <Integer, Integer> requests = new ConcurrentHashMap<Integer, Integer>();    
    
    private static AtomicInteger num_updates = new AtomicInteger();
    private static AtomicInteger num_searches = new AtomicInteger();
    
    private static void init(MongoAppClient[] clients, int num_clients) {
		if (System.getProperty("ratio") != null) {
			ratio = Double.parseDouble(System.getProperty("ratio"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
		String fileName = MongoAppClient.getKeyFilename();
		
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
				
		for (int i=0; i<num_clients; i++) {
			try {
				clients[i] = new MongoAppClient();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
			int req_id = getReqId();
			requests.put(req_id, 1);
			
			String serviceName = client.getServiceName(oldVal);
			
			try {
				client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
						new RequestCallback() {
					@Override
					public void handleResponse(Request response) {
						synchronized (client) {
							requests.remove(req_id);
							num_updates.incrementAndGet();
							client.incrTotalRcvd(1);
							client.notifyAll();
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
			int num_selected_attr = 1; //rand.nextInt(num_attributes)+1;
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
			int req_id = getReqId();
			requests.put(req_id, numPartition);
			
			// Send to all partitions in a replica
			long start = System.currentTimeMillis();
			int idx = rand.nextInt(numReplica);
			for (int i=0; i<numPartition; i++){
				String serviceName = MongoAppClient.allServiceNames.get(i);
				List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
				InetSocketAddress addr = l.get(idx);
				AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
				request.setNeedsCoordination(false);
				
				try {				
					client.sendRequest(request, addr,
							new RequestCallback() {
						@Override
						public void handleResponse(Request response) {
							synchronized (client) {
								int left = requests.get(req_id);
								left--;
								if(left == 0){
									DelayProfiler.updateDelay("latency", start);
									requests.remove(req_id);
									num_searches.incrementAndGet(); 
								} else {
									requests.put(req_id, left);
								}
								client.incrTotalRcvd(1);
								client.notifyAll();
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
    	MongoAppClient[] clients = new MongoAppClient[num_clients];
		
    	init(clients, num_clients);
    	
		// Create all service names
		MongoAppClient.createAllGroups(clients[0], false);		
		// Thread.sleep(2000);		
		
    	ExecutorService executor = Executors.newFixedThreadPool(100);

    	long begin = System.currentTimeMillis();
		long stop = begin + MongoApp.EXP_TIME;
		
		int cnt = 0;
    	while ( System.currentTimeMillis() < stop ) {
    		MongoAppClient client = clients[cnt % num_clients];
    		
    		while(!client.isReady()){
				synchronized(client){
					try {
						client.wait(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}		
    		
    		if(rand.nextDouble() < ratio){ 
    			client.incrTotalSent(1);
    			executor.execute(new UpdateThread(client));
    		} else {
    			client.incrTotalSent(numPartition);
    			executor.execute(new SearchThread(client));
    		}
    		
    		cnt++;		
    	}
    	
    	System.out.println("Elapsed: "+(System.currentTimeMillis()-begin)/1000.0+"");
		System.out.println("Thruput: "+(num_searches.get()+num_updates.get())*1000.0/(MongoApp.EXP_TIME));
		System.out.println("DELAY:"+DelayProfiler.getStats());
		
		
		for (int i=0; i<num_clients; i++){
			clients[i].close();
		}
		executor.shutdown();
    }
}
