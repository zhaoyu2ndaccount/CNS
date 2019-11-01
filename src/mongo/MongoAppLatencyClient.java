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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.reconfiguration.examples.AppRequest;


/**
 * @author gaozy
 */
public class MongoAppLatencyClient {

	private static ExecutorService executor;
	
 	private static Random rand = new Random();

	private static double ratio = 0.0;

	private static double fraction = 0.000005;
	
	private static int selected_attr = 0;

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
	
	private static int num_clients = 50;
	
	private static int numReplica;
	private static int numPartition;
    
    // probing related variables
    // private static AtomicInteger num_updates = new AtomicInteger();
    // private static AtomicInteger num_searches = new AtomicInteger();
    // private static AtomicInteger num_searches_single = new AtomicInteger();
    // private static AtomicLong totalLatency = new AtomicLong();
    private static AtomicLong searchID = new AtomicLong();
    // private static long lastResponseReceivedTime = System.currentTimeMillis();    
    private static final ConcurrentHashMap<Long, Integer> requestMap = new ConcurrentHashMap<>();
    
    
    private static MongoAppClient[] init() {
		if (System.getProperty("ratio") != null) {
			ratio = Double.parseDouble(System.getProperty("ratio"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
		if (System.getProperty("numClients") != null){
			num_clients = Integer.parseInt(System.getProperty("numClients"));
		}
		
		if (System.getProperty("selected") != null){
			selected_attr = Integer.parseInt(System.getProperty("selected"));
		}
		
		String fileName = MongoAppClient.getKeyFilename();
		
		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	keys.add(line);
		    }
		    br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (int k=0; k<num_attributes; k++) {
			attributes.add(ATTR_PREFIX+k);
		}
		
		numReplica = (System.getProperty("numReplica")!=null)?
				Integer.valueOf(System.getProperty("numReplica")) : 1;	
		numPartition = (System.getProperty("numPartition")!=null)?
				Integer.valueOf(System.getProperty("numPartition")): 1;
								
		MongoAppClient[] clients = new MongoAppClient[num_clients];
		for (int i=0; i<num_clients; i++) {
			try {
				clients[i] = new MongoAppClient();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return clients;
	}
    
	private static void sendUpdateRequest(MongoAppClient client){ 
		String key = keys.get(rand.nextInt(keys.size()));
		Document oldVal = new Document();
		oldVal.put(MongoApp.KEYS.KEY.toString(), key);
		Document newVal = new Document();
		
		for (int k=0; k<num_attributes; k++) {
			// newVal.put(ATTR_PREFIX+k, rand.nextInt(max_val)+rand.nextDouble());
			newVal.put(ATTR_PREFIX+k, rand.nextInt());
		}
		
		JSONObject req = MongoAppClient.replaceRequest(oldVal, newVal);
		
		String serviceName = client.getServiceName(oldVal);
		final long start = System.currentTimeMillis();
		
		try {
			RequestFuture future = client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
					new RequestCallback() {
				@Override
				public void handleResponse(Request response) {											
					long elapsed = System.currentTimeMillis() - start;
					System.out.println(elapsed);
					System.out.flush();
				}
			});
			future.get();
			
		} catch (IOException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		
		
    }
    
	private static void sendSearchRequest(MongoAppClient client) {
		List<String> givenList = new ArrayList<String>(attributes);
		Collections.shuffle(givenList);
		
		int num_selected_attr = selected_attr;
		// select only one attribute
		if (selected_attr == 0 ) {
			num_selected_attr = rand.nextInt(3)+1;
		}
		List<String> attr_selected = givenList.subList(0, num_selected_attr);
		// Collections.sort(attr_selected);
		
		// int drift = (int) (2*Math.round(max_val*Math.pow(fraction, 1.0/num_selected_attr)));
		double drift = max_val*Math.pow(fraction, 1.0/num_selected_attr);
		
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
		JSONObject reqVal = MongoAppClient.findRequest(query, max_batch/numPartition);
		// Send to all machines in a replica
		int idx = rand.nextInt(numReplica);
		
		// AtomicInteger resp = new AtomicInteger();
		final Long requestID = searchID.getAndIncrement();
		requestMap.put(requestID, numPartition);
		final long start = System.currentTimeMillis();
		
		List<RequestFuture> futures = new ArrayList<>();
		
		for (int i=0; i<numPartition; i++){
			String serviceName = MongoAppClient.allServiceNames.get(i);
			List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
			InetSocketAddress addr = l.get(idx);
			AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
			request.setNeedsCoordination(false);
			try {				
				RequestFuture<Request> future = client.sendRequest(request, addr,
						new RequestCallback() {
					@Override
					public void handleResponse(Request response) {											
						synchronized(requestID) {
							int left = requestMap.get(requestID) - 1;
							if (left == 0){
								long elapsed = System.currentTimeMillis() - start;
								System.out.println(elapsed);
							} else {
								requestMap.put(requestID, left);								
							}
						}						
						// num_searches_single.incrementAndGet();
					}
				});
				futures.add(future);
									
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
						
		}
		
		for (RequestFuture future : futures){
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
			    	
    }
    
    
    private static int sendRequests(int numReqs,
			MongoAppClient[] clients, boolean searchOnly) {
    	
    	int cnt = 0;
    	// RateLimiter rateLimiter = new RateLimiter(rate);
		for (int i=0; i<numReqs; i++) {
			
			if (rand.nextDouble() < ratio){
				sendUpdateRequest( clients[i % num_clients]);
			}
			else{				
				sendSearchRequest( clients[i % num_clients]);					
			}
			
			// rateLimiter.record();
		}
  	    	
    	return cnt;
    }
    
    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws IOException, InterruptedException {    	
    	
    	// Initialize all parameters and create clients 
    	MongoAppClient[] clients = init();
    	
		// Create all service names locally
		MongoAppClient.createAllGroups(clients[0], false);		
		// Thread.sleep(2000);		
		
		// initialize executor pool
    	executor = Executors.newFixedThreadPool(num_clients*10);
    	
    	// begin latency test	
		
		int numWarmupRequests = 10000;
		
		sendRequests(numWarmupRequests, clients, true);
		
		// end latency test		
		
		for (int i=0; i<num_clients; i++){
			clients[i].close();
		}
		executor.shutdown();
		Thread.sleep(1000);
		System.exit(0);
    }
}
