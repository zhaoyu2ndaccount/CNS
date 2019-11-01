package mongo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.gigapaxos.interfaces.RequestFuture;
import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.utils.Util;


/**
 * @author gaozy
 */
public class MongoAppFaultTolerantClient {

	private static ExecutorService executor;
	
 	private static Random rand = new Random();

	private static double ratio = 0.0;

	private static double fraction = 0.000005;
	
	private static int selected_attr = 0;
	
	private static int EXP_TIME = 120;
	private static int TIMEOUT_INTERVAL = 5; // 8 seconds for ping timeout

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

    // probing related variables
    private static AtomicInteger num_updates = new AtomicInteger();
    private static AtomicInteger num_searches = new AtomicInteger();
    private static AtomicInteger num_searches_single = new AtomicInteger();
    private static AtomicLong totalLatency = new AtomicLong();
    private static AtomicLong searchID = new AtomicLong();
    private static long lastResponseReceivedTime = System.currentTimeMillis();    
    private static final ConcurrentHashMap<Long, Integer> requestMap = new ConcurrentHashMap<>();
    
    private static ScheduledExecutorService scheduler;
    
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
		// newVal.put(MongoApp.KEYS.KEY.toString(), key);
		for (int k=0; k<num_attributes; k++) {
			newVal.put(ATTR_PREFIX+k, rand.nextInt());
		}
		
		JSONObject req = MongoAppClient.replaceRequest(oldVal, newVal);
		
		// String serviceName = client.getServiceName(oldVal);
		// FIXME: this only works with the @scheme.ConsistentHash scheme
		byte[] b = key.getBytes();
		int retval = b.hashCode();
		// group index
		int groupIdx = retval % MongoAppClient.allServiceNames.size(); 
		
		String serviceName = MongoAppClient.allServiceNames.get(groupIdx);
		
		// partition index
		int idx = client.getReplicaIndex();
		InetSocketAddress addr = MongoAppClient.allGroups.get(groupIdx).get(idx);
		
		try {
			client.sendRequest(new AppRequest(serviceName, req.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
					addr,
					new RequestCallback() {
				@Override
				public void handleResponse(Request response) {
					lastResponseReceivedTime = System.currentTimeMillis();
					num_updates.incrementAndGet();
					
				}
			});
			
		} catch (IOException e) {
			// e.printStackTrace();			
			MongoAppClient.updateReplicaState(idx, false, addr);
		}
    	
    }
    
    @SuppressWarnings("unchecked")
	private static void sendSearchRequest(MongoAppClient client) {
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
		
		// Request packet
		JSONObject reqVal = MongoAppClient.findRequest(query, max_batch/numPartition);		
		
		// Send to all machines in a replica, only pick the replica that has all partitions available
		int idx = client.getReplicaIndex(); // rand.nextInt(numReplica);
		
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
						lastResponseReceivedTime = System.currentTimeMillis();						
						synchronized(requestID) {
							int left = requestMap.get(requestID) - 1;
							if (left == 0){
								num_searches.incrementAndGet();
							} else {
								requestMap.put(requestID, left);
							}
						}						
						// num_searches_single.incrementAndGet();
					}
				});
									
			} catch (IOException e) {
				// e.printStackTrace();
				MongoAppClient.updateReplicaState(idx, false, addr); 
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
				sendSearchRequest( clients[i % num_clients] );
				rateLimiter.record();
			}
    	} else {
    		for (int i=0; i<numReqs; i++) {
    			if (rand.nextDouble() < ratio){
    				sendUpdateRequest( clients[i % num_clients]);
    				update++;
    			}
    			else
    				sendSearchRequest( clients[i % num_clients]);
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
				// TODO Auto-generated catch block
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
    
    
    /**
     * We need this thread to periodically detect a failure or a recovery
     * @author gaozy
     */
    static class FailureDetector implements Runnable {
    	MongoAppClient[] clients;
    	int num_clients;
    	ExecutorService executor;
    	    	
    	FailureDetector(MongoAppClient[] clients, ExecutorService executor) {
    		this.clients = clients;
    		this.num_clients = clients.length;
    		this.executor = executor;
    	}
    	
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			// keep alive interval
	    	int keepAliveInterval = 10;
			long end = System.currentTimeMillis()+(EXP_TIME+10)*1000;
			
	    	while (System.currentTimeMillis() < end) { 
	    		long begin = System.currentTimeMillis();
	    		
	 			List<RequestFuture> requests = new ArrayList<RequestFuture>();
				
				// FIXME: ping all servers?			
				for (int k=0; k<numReplica; k++) {
					for (int i = 0; i< numPartition; i++) {
						MongoAppClient client = clients[i % num_clients];
						JSONObject reqVal = MongoAppClient.pingRequest();
						String serviceName = MongoAppClient.allServiceNames.get(i);
						List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
						InetSocketAddress addr = l.get(k);
						AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
						request.setNeedsCoordination(false);
						
						try {						
							requests.add(client.sendRequest(request, addr, 
									new RequestCallback() {							
										@Override
										public void handleResponse(Request response) {
											
										}							
									})
							);
							
						} catch (IOException e) {
							// do nothing
							// e.printStackTrace();
							// MongoAppClient.updateReplicaState(i, false, addr);
						} 
					}
				}
				Iterator<RequestFuture> iter = requests.iterator();
				
				for (int k=0; k<numReplica; k++) {
					for (int idx = 0; idx< numPartition; idx++) {
						final int partitionIdx = k;
						final int i = idx;
						Runnable runnable = () -> {
							RequestFuture future = iter.next();
							List<InetSocketAddress> l = MongoAppClient.allGroups.get(i);
							InetSocketAddress addr = l.get(partitionIdx);				
							try {
								future.get(TIMEOUT_INTERVAL, TimeUnit.SECONDS);
								if ( future.isDone() ){
									// System.out.println("Ping ("+partitionIdx+","+i+"): alive");
									MongoAppClient.updateReplicaState(i, true, addr);
								}
							} catch (InterruptedException | ExecutionException | TimeoutException e) {
								MongoAppClient.updateReplicaState(i, false, addr);
								// e.printStackTrace();
								// System.out.println("Ping ("+partitionIdx+","+i+"): down");
							}
						};
						
						executor.submit(runnable);
						// new Thread(runnable).start();
					}
				}
				
				long elapsed = System.currentTimeMillis() - begin;
				long toWait = keepAliveInterval*1000 - elapsed;
				if (toWait > 0 ) {
					try {
						Thread.sleep(toWait);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
	    	}
		}
    	
    }
    
    static class StatRunnable implements Runnable{

		@Override
		public void run() {
			long start = System.currentTimeMillis();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			int last = 0;			
			long endTime = System.currentTimeMillis() + EXP_TIME*1000;
			while(System.currentTimeMillis() < endTime){
				long begin = System.currentTimeMillis();
				int total = num_searches.get() + num_updates.get();
				double moving_avg = total*1000.0 / (begin - start) ;
				int throughput = total - last;
				System.out.println("Thruput: "+throughput
						+", moving_avg="+Util.df(moving_avg)
						+", searches="+num_searches.get()+", updates="+num_updates.get()
						+"\t "+"["+new java.util.Date(System.currentTimeMillis())+"].");
				
				last = total;
				long elapsed = System.currentTimeMillis() - begin;
				if (elapsed < 1000){
					try {
						Thread.sleep(1000-elapsed);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
    	int sendingRate = 1000;
    	if (args.length > 0)
    		sendingRate = Integer.parseInt(args[0]);
    	
    	scheduler = Executors.newScheduledThreadPool(40);
    	 
    	// Initialize all parameters and create clients 
    	MongoAppClient[] clients = init();
    	
		// Create all service names locally
		MongoAppClient.createAllGroups(clients[0], false);		
		// Thread.sleep(2000);		
		
		// initialize executor pool
    	executor = Executors.newFixedThreadPool(num_clients);    	
    	
    	// 
    	Thread th = new Thread(new StatRunnable());
    	th.start();  
    	
    	FailureDetector detector = new FailureDetector(clients, executor);
    	Thread detector_th = new Thread(detector);
    	detector_th.start();
    	
    	// begin run
    	
		int numRequests = sendingRate*EXP_TIME;
		sendRequests(numRequests, clients, false,
				sendingRate);
		waitForResponses(numRequests);
		
    	
		for (int i=0; i<num_clients; i++){
			clients[i].close();
		}
		executor.shutdown();
		
    }
}
