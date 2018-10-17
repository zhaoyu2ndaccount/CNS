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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.Util;


/**
 * @author gaozy
 */
public class MongoAppCapacityProbingClient {

	private static ExecutorService executor;
	
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
	
	private static int num_clients = 20;
	
	private static int numReplica;
	private static int numPartition;
    
    private static int total_reqs = MongoApp.TOTAL_REQS;
    
    // probing related variables
    private static AtomicInteger num_updates = new AtomicInteger();
    private static AtomicInteger num_searches = new AtomicInteger();
    private static AtomicLong totalLatency = new AtomicLong();
    private static long lastResponseReceivedTime = System.currentTimeMillis();    
    
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
				
				
		MongoAppClient[] clients = new MongoAppClient[num_clients];
		for (int i=0; i<num_clients; i++) {
			try {
				clients[i] = new MongoAppClient();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return clients;
	}
    
    @SuppressWarnings("unchecked")
	private static void sendUpdateRequest(MongoAppClient client){ 
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
		
		try {
			client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
					new RequestCallback() {
				@Override
				public void handleResponse(Request response) {
					lastResponseReceivedTime = System.currentTimeMillis();
					num_updates.incrementAndGet();
					
				}
			});
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    @SuppressWarnings("unchecked")
	private static void sendSearchRequest(MongoAppClient client) {
		List<String> givenList = new ArrayList<String>(attributes);
		Collections.shuffle(givenList);
		
		// select only one attribute
		int num_selected_attr = 1; //rand.nextInt(num_attributes)+1;
		List<String> attr_selected = givenList.subList(0, num_selected_attr);
		
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
		JSONObject reqVal = MongoAppClient.findRequest(query, max_batch);
		// Send to all machines in a replica
		int idx = rand.nextInt(numReplica);
		
		// AtomicInteger resp = new AtomicInteger();
		
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
						lastResponseReceivedTime = System.currentTimeMillis();
						num_searches.incrementAndGet();
					}
				});
									
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
						
		}
			    	
    }
    
    
    private static void sendRequests(int numReqs,
			MongoAppClient[] clients, boolean searchOnly, double rate) {
    	RateLimiter rateLimiter = new RateLimiter(rate);
    	if(searchOnly){
    		// only send search for warmup requests	    	
	    	for (int i = 0; i < numReqs; i++) {
				sendSearchRequest( clients[i % num_clients] );
				rateLimiter.record();
			}
    	} else {
    		for (int i=0; i<numReqs; i++) {
    			if (rand.nextDouble() < ratio)
    				sendUpdateRequest( clients[i % num_clients]);
    			else
    				sendSearchRequest( clients[i % num_clients]);
    			rateLimiter.record();
    		}
    	}
    }
    
    protected static boolean waitForResponses(MongoAppClient[] clients,
			long startTime, int numRequests) {
    	// wait for EXP_WAIT_TIME if 99.9% requests not coming back
    	while(System.currentTimeMillis() - startTime < MongoApp.EXP_WAIT_TIME 
    			&& (num_searches.get()/numPartition + num_updates.get() ) < numRequests*0.999 ) {
    		System.out.println("Total:"+numRequests+", search:"+num_searches.get()/numPartition+",update:"+num_updates.get());
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	if ((num_searches.get()/numPartition + num_updates.get() ) < numRequests*0.999)
    		return false;
    	return true;
    }
    
    private static double startProbing(double load, MongoAppClient[] clients) throws InterruptedException {
    	long runDuration = MongoApp.PROBE_RUN_DURATION; // seconds
		double responseRate = 0, capacity = 0, latency = Double.MAX_VALUE;
		double threshold = MongoApp.PROBE_RESPONSE_THRESHOLD;
		double loadIncreaseFactor = MongoApp.PROBE_LOAD_INCREASE_FACTOR, minLoadIncreaseFactor = 1.01;
		int runs = 0, consecutiveFailures = 0;
		double realCapacity = 0;
		
		do {
			if (runs++ > 0)
				// increase probe load only if successful
				if (consecutiveFailures == 0)
					load *= loadIncreaseFactor;
				else
					// scale back if failed
					load *= (1 - (loadIncreaseFactor - 1) / 2);

			/* Two failures => increase more cautiously. Sometimes a failure
			 * happens in the very first run if the JVM is too cold, so we wait
			 * for at least two consecutive failures. */
			if (consecutiveFailures == 2)
				loadIncreaseFactor = (1 + (loadIncreaseFactor - 1) / 2);

			// we are within roughly 0.1% of capacity
			if (loadIncreaseFactor < minLoadIncreaseFactor)
				break;
			
			//reset latency and counters
			totalLatency.set(0);
			num_searches.set(0);
			num_updates.set(0);
			
			
			int numRunRequests = (int) (load * runDuration);
			long t1 = System.currentTimeMillis();
			sendRequests(numRunRequests, clients, false, load);

			// no need to wait for all responses
			while (num_searches.get()/numPartition + num_updates.get() < threshold * numRunRequests)
				Thread.sleep(500);
			
			int numResponses = num_searches.get()/numPartition + num_updates.get();
			System.out.println("Number of responses:"+numResponses+", search:"+num_searches.get()+", update:"+num_updates.get());
			
			responseRate = // numRunRequests
			numResponses * 1000.0 / (lastResponseReceivedTime - t1);
			
			latency =  totalLatency.get() * 1.0 / numResponses;
			
			if (latency < MongoApp.PROBE_LATENCY_THRESHOLD)
				capacity = Math.max(capacity, responseRate);
			boolean success = (responseRate > threshold * load && 
					latency <= MongoApp.PROBE_LATENCY_THRESHOLD);
			System.out.println("capacity >= " + Util.df(capacity)
					+ "/s; (response_rate=" + Util.df(responseRate)
					+ "/s, average_response_time=" + Util.df(latency) + "ms)"
					+ (!success ? "    !!!!!!!!FAILED!!!!!!!!" : ""));
			Thread.sleep(2000);
			if (success){
				consecutiveFailures = 0;
				realCapacity = capacity;
			}
			else{
				consecutiveFailures++;	
				Thread.sleep(2000);
			}
		} while (consecutiveFailures < MongoApp.MAX_CONSECURIVE_FAILURES && runs < MongoApp.MAX_RUN_ATTEMPTS); 
		
		System.out
		.println("capacity <= "
				+ Util.df(Math.max(capacity, load))
				+ ", stopping probes because"
				+ (capacity < threshold * load ? " response_rate was less than 95% of injected load"
						+ Util.df(load) + "/s; "
						: "")
				+ (latency > MongoApp.PROBE_LATENCY_THRESHOLD ? " average_response_time="
						+ Util.df(latency)
						+ "ms"
						+ " >= "
						+ MongoApp.PROBE_LATENCY_THRESHOLD
						+ "ms;"
						: "")
				+ (loadIncreaseFactor < minLoadIncreaseFactor ? " capacity is within "
						+ Util.df((minLoadIncreaseFactor - 1) * 100)
						+ "% of next probe load level;"
						: "")
				+ (consecutiveFailures > MongoApp.MAX_CONSECURIVE_FAILURES ? " too many consecutive failures;"
						: "")
				+ (runs >= MongoApp.MAX_RUN_ATTEMPTS ? " reached limit of "
						+  MongoApp.MAX_RUN_ATTEMPTS
						+ " runs;"
						: ""));
		return realCapacity;
				
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
    	executor = Executors.newFixedThreadPool(num_clients);
    	
    	// begin warmup run		
		long t1 = System.currentTimeMillis();
		int numWarmupRequests = Math.min(total_reqs, 10 * num_clients);
		sendRequests(numWarmupRequests, clients, true,
				10 * num_clients);
		boolean success = waitForResponses(clients, t1, numWarmupRequests);
		System.out.println("[success]");		
		// end warmup run
		
		int probing_start_point = 1000 * numPartition;
		
		// begin probing
		double responseRate = startProbing(probing_start_point, clients);
		// end probing
		System.out.println("Thruput: "+Util.df(responseRate));
		
		for (int i=0; i<num_clients; i++){
			clients[i].close();
		}
		executor.shutdown();
    }
}
