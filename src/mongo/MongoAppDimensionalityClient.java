package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.Util;
import org.bson.Document;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author gaozy
 */
public class MongoAppDimensionalityClient {

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

	// the value range to touch in a single search query
	private final static double DRIFT = max_val*fraction;

	// max batch number
	private final static int max_batch = MongoApp.MAX_BATCH_NUM;
	
	private static List<String> keys = new ArrayList<String>();
	
	private static List<String> attributes = new ArrayList<String>();
	
	private static int num_clients = 50;
	
	private static int numReplica;
	private static int numPartition;
    
    private static int total_reqs = MongoApp.TOTAL_REQS;
    
    // probing related variables
    private static AtomicInteger num_updates = new AtomicInteger();
    private static AtomicInteger num_searches = new AtomicInteger();
    private static AtomicInteger num_searches_single = new AtomicInteger();
    private static AtomicLong totalLatency = new AtomicLong();
    private static AtomicLong searchID = new AtomicLong();
    private static long lastResponseReceivedTime = System.currentTimeMillis();    
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
			e.printStackTrace();
		}
    	
    }
    
    private static void sendSearchRequestDirectlyToMongo() {
    	List<String> givenList = new ArrayList<String>(attributes);
		Collections.shuffle(givenList);
		
		int num_selected_attr = selected_attr;
		// select only one attribute
		if (selected_attr == 0 ) {
			num_selected_attr = rand.nextInt(3)+1;
		}
		List<String> attr_selected = givenList.subList(0, num_selected_attr);
		Collections.sort(attr_selected);
		
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
    	
    	int idx = rand.nextInt(numReplica);
    	
    	Set<String> result = new HashSet<String>();
    	final Long requestID = searchID.getAndIncrement();
		requestMap.put(requestID, numPartition);
		
		for ( int i=0; i<numPartition; i++ ) {			
			MongoCollection<Document> collection = MongoAppClient.allCollections.get(i*numReplica+idx);
			
			Runnable runnable = () -> {
				// MongoCursor<Document> cursor = collection.find(query).projection(Projections.include(MongoApp.KEYS.KEY.toString())).limit(50).iterator(); 
				MongoCursor<Document> cursor = collection.find(query).projection(Projections.include(MongoApp.KEYS.KEY.toString())).iterator();
				try {
				    while (cursor.hasNext()) {
				    	// This is a string
				        // cursor.next().toJson();
				    	String record = cursor.next().toJson();
				        result.add(record);
				    }
				} finally {
					synchronized(requestID) {
						int left = requestMap.get(requestID) - 1;
						if (left == 0){
							num_searches.incrementAndGet();
						} else {
							requestMap.put(requestID, left);
							lastResponseReceivedTime = System.currentTimeMillis();
						}
					}
				    cursor.close();
				}
			};
			executor.submit(runnable);
			// Thread thread = new Thread(runnable);
			// thread.start();
		}
    }

	/**
	 *
	 * @param client
	 */
	private static void sendSearchRequestForDimensionality(MongoAppClient client) {
		List<String> givenList = new ArrayList<String>(attributes);
		
		int num_selected_attr = selected_attr;
		// select 1-3 random number of attributes
		if (selected_attr == 0 ) {
			num_selected_attr = rand.nextInt(3)+1;
		}
		List<String> attr_selected = givenList.subList(0, num_selected_attr);
		// Collections.sort(attr_selected);
		
		// int drift = (int) (2*Math.round(max_val*Math.pow(fraction, 1.0/num_selected_attr)));
		double drift = DRIFT;
		
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
		
		for (int i=0; i<numPartition; i++){
			String serviceName = MongoAppClient.allServiceNames.get(i);
			List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
			InetSocketAddress addr = l.get(idx);
			AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
			request.setNeedsCoordination(false);
			// request.getRequestID()
			try {				
				client.sendRequest(request, addr,
						new RequestCallback() {
					@Override
					public void handleResponse(Request response) {											
						synchronized(requestID) {
							int left = requestMap.get(requestID) - 1;
							if (left == 0){
								num_searches.incrementAndGet();
								lastResponseReceivedTime = System.currentTimeMillis();	
							} else {
								requestMap.put(requestID, left);								
							}
						}						
						// num_searches_single.incrementAndGet();
					}
				});
									
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
						
		}
			    	
    }
    
    
    private static int sendRequests(int numReqs,
			MongoAppClient[] clients, boolean searchOnly, double rate) {
    	
    	int update = 0;
    	RateLimiter rateLimiter = new RateLimiter(rate);
    	if(searchOnly){
    		// only send search for warmup requests	    	
	    	for (int i = 0; i < numReqs; i++) {
	    		if (MongoAppClient.through_paxos)
	    			sendSearchRequestForDimensionality( clients[i % num_clients] );
	    		else
	    			sendSearchRequestDirectlyToMongo();
				rateLimiter.record();
			}
    	} else {
    		for (int i=0; i<numReqs; i++) {
    			if (rand.nextDouble() < ratio){
    				sendUpdateRequest( clients[i % num_clients]);
    				update++;
    			}
    			else{
    				if (MongoAppClient.through_paxos)
    					sendSearchRequestForDimensionality( clients[i % num_clients]);
    				else
    					sendSearchRequestDirectlyToMongo();
    			}
    			rateLimiter.record();
    		}
    	}
    	return update;
    }
    
    private static boolean waitForResponses( int numRequests) {
        return waitForResponses(numRequests, 0.999, 0);
    }
    
    protected static boolean waitForResponses(int numRequests, double frac, int update) {
    	// long startTime = System.currentTimeMillis();
    	// wait for EXP_WAIT_TIME if 99.9% requests not coming back
    	int last = 0;
    	int cnt = 0;
    	while(cnt < 10 &&
    	         (num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition) < numRequests*frac ) {
    		System.out.println("Total:"+numRequests+"("+(numRequests-update)+","+update+"), search:"+num_searches.get()+",update:"+num_updates.get()
    		+", single search:"+num_searches_single.get()/numPartition);
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		if ((num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition) == last)
    			++cnt;
    		else
    			cnt = 0;
    		last = (num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition);
    	}
    	if ((num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition) < numRequests*0.999)
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
			num_searches_single.set(0);
			
			int numRunRequests = (int) (load * runDuration);
			long t1 = System.currentTimeMillis();
			int update = sendRequests(numRunRequests, clients, false, load);

			// no need to wait for all responses
			/*
			while (num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition < threshold * numRunRequests)
				Thread.sleep(500);
			*/
			
			int numResponses = num_searches.get() + num_updates.get() + num_searches_single.get()/numPartition;
			System.out.println("Number of responses:"+numResponses+", search:"+num_searches.get()+", update:"+num_updates.get()
			+", single search:"+num_searches_single.get()/numPartition);
			
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
			
			// Thread.sleep(2000);
			if (success){
				consecutiveFailures = 0;
				realCapacity = capacity;
			}
			else{
				consecutiveFailures++;	
				Thread.sleep(2000);
			}
			boolean finished = waitForResponses(numRunRequests, 0.995, update);
            if( !finished ){
                // after wait time, not all requests come back, decrease the load a little bit more
                // load *= (1 - (loadIncreaseFactor - 1) / 2);
            	
            	// wait for 2 seconds if there are still requests unfinished
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
    	executor = Executors.newFixedThreadPool(num_clients*10);
    	
    	int probing_start_point = 200*numReplica;
    	if (args.length > 0)
    		probing_start_point = Integer.parseInt(args[0]);
    	
    	// begin warmup run
		int numWarmupRequests = Math.min(total_reqs, 10 * num_clients);
		
		sendRequests(numWarmupRequests, clients, true,
				probing_start_point);
		boolean success = waitForResponses(numWarmupRequests);
		
        if (success)
            System.out.println("[success]");
        else {
            System.out.println("[failure]");
            System.out.println("Warm up failed. Ready to exit...");
            System.exit(-1);
        }
		// end warmup run
		
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
