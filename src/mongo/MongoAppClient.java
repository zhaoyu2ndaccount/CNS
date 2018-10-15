package mongo;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import schemes.BasicScheme;

/**
 * @author gaozy
 *
 */
@SuppressWarnings("rawtypes")
public class MongoAppClient extends ReconfigurableAppClientAsync implements AppRequestParserBytes{

	private static BasicScheme scheme;
	protected static Map<Integer, String> allServiceNames;
	protected static Map<Integer, List<InetSocketAddress>> allGroups;
	private static Map<Integer, MongoCollection<Document>> allCollections;
	private static int numPartition;
	private static int numReplica;
	private static String key_filename = "keys.txt";
	
	private static boolean through_paxos = true;
	
	private static boolean initialized = false;
	
	private static final Logger log = Logger
			.getLogger(ReconfigurableAppClientAsync.class.getName());
	
	private static final String DEFAULT_SCHEME_NAME = "schemes.ConsistentHash";
	private static final int DEFAULT_NUM_PARTITION = 1;
	private static final int DEFAULT_NUM_REPLICA = 1;
	private static final int DEFAULT_KEY_LEN = 32;
	private static final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	
	private static final Random rand = new Random();
	
	private static int key_length = DEFAULT_KEY_LEN;
	
	private int total_sent = 0;
	synchronized void incrTotalSent(int num) {
		total_sent += num;
	}
	
	private int total_rcvd = 0;
	synchronized void incrTotalRcvd(int num) {
		total_rcvd += num;
	}
	
	private final static int num_outstanding_request_per_client = 4000;
	synchronized boolean isReady() {
		return total_rcvd + num_outstanding_request_per_client > total_sent? true:false;
	}
	
	
	/**
	 * All MongoAppClient share the same configuration
	 */
	protected static void init() {
		// set key length, otherwise key length is DEFAULT_KEY_LEN
		if(System.getProperty("keyLength") != null){
			key_length = Integer.valueOf(System.getProperty("keyLength"));
		}
		
		if(System.getProperty("keyFilename") != null) {
			key_filename = System.getProperty("keyFilename");
		}
		
		if (System.getProperty("paxos") != null) {
			through_paxos = Boolean.parseBoolean(System.getProperty("paxos"));
		}
		
		numPartition = (System.getProperty("numPartition")!=null)?
				Integer.valueOf(System.getProperty("numPartition")):DEFAULT_NUM_PARTITION;
		numReplica = (System.getProperty("numReplica")!=null)?
				Integer.valueOf(System.getProperty("numReplica")):DEFAULT_NUM_REPLICA;	
				
		// set scheme
		String schemeName = System.getProperty("scheme");
		if (schemeName == null) {
			schemeName = DEFAULT_SCHEME_NAME;
		}
				
		// initialize scheme
		try {			
			Class<?> c = Class.forName(schemeName);			
			scheme = (BasicScheme) c.getConstructor(int.class, int.class).newInstance(numPartition, numReplica);
			
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		
		// initialize server map
		allGroups = getGroupForAllServiceNames();
		for (int id: allGroups.keySet()) {
			System.out.println("Group "+id+":"+allGroups.get(id));
		}
		
		allServiceNames = new HashMap<Integer, String>();
		for (int i:allGroups.keySet()) {
			allServiceNames.put(i, PaxosConfig.getDefaultServiceName()+i);
			System.out.println("Service name "+i+":"+PaxosConfig.getDefaultServiceName()+i);
		}
		
		
		
		Map<String, InetSocketAddress> actives = PaxosConfig.getActives();		
		int num_actives = actives.keySet().size();
		
		if (num_actives > numPartition*numReplica) {
			log.log(Level.FINE, "The number of Paxos instances("+num_actives+
					") is more than the total number required ("+numPartition*numReplica+").");
		} else if (num_actives < numPartition*numReplica) {
			log.log(Level.WARNING, "The number of Paxos instances("+num_actives+
					") is less than the total number required ("+numPartition*numReplica+").");
		}
		
		log.log(Level.FINE, "MongoAppClient is initialized successfully.");
		
		allCollections = new HashMap<Integer, MongoCollection<Document>>();
		int port = 27017;
		int id = 0;
		for (String name: actives.keySet()) {
			String table_name = name;
			String host = actives.get(name).getHostName(); // ((List<InetSocketAddress>) allGroups.get(id)).getHostName();
			
			MongoClient mongoClient = new MongoClient(host, port);
			MongoDatabase database = mongoClient.getDatabase(MongoDBServiceApp.DB_NAME);
			MongoCollection<Document> collection = database.getCollection(table_name);
			
			allCollections.put(id, collection);
			++id;
		}
		
		initialized = true;
	}
	
	/**
	 * @throws IOException
	 */
	public MongoAppClient() throws IOException {
		super();
		
		if(!initialized)
			init();
	}
	
	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return NoopApp.staticGetRequest(stringified);
		} catch (RequestParseException | JSONException e) {
			e.printStackTrace();
			// do nothing by design
		}
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return NoopApp.staticGetRequestTypes();
	}
	
	protected static String getKeyFilename() {
		return key_filename;		
	}
	
	protected static int getKeyLength() {
		return key_length;
	}
	
	
	protected static Map<Integer, List<InetSocketAddress>> getGroupForAllServiceNames() {		
		return scheme.getGroupForAllServiceNames(numPartition, numReplica, PaxosConfig.getActives());
	}
	
	protected String getServiceName(Document bson) {
		return scheme.getServiceName(bson, allServiceNames);
	}

	protected List<InetSocketAddress> getServiceGroup(Document bson) {
		return scheme.getServiceGroup(bson, allGroups);
	}
	
	protected static void createAllGroups(MongoAppClient client, boolean toSent) throws IOException, InterruptedException {
		for (int i:allGroups.keySet()) {
//			System.out.println("Group "+i+":"+allGroups.get(i));
			Set<InetSocketAddress> initGroup = new HashSet<InetSocketAddress>(allGroups.get(i));
			// put into service name map
//			allServiceNames.put(i, PaxosConfig.getDefaultServiceName()+i);
			
			if (toSent) {
				incrSent(1);
				client.sendRequest(new CreateServiceName(PaxosConfig.getDefaultServiceName()+i, 
						"", initGroup), 
						new RequestCallback() {
							long createTime = System.currentTimeMillis();
							@Override
							public void handleResponse(Request response) {
								System.out
								.println("Response to create service name ["+PaxosConfig.getDefaultServiceName()+i
										+ "] = "
										+ (response)
										+ " received in "
										+ (System.currentTimeMillis() - createTime)
										+ "ms");
								incrRcvd();
							}
						}
					);
			}
			while(sent < received) {
				Thread.sleep(500);
			}
		}	
			
	}
	
	
	
	@SuppressWarnings({ "unchecked" })
	protected Set<String> sendFindRequestAsync(BasicDBObject query) {
		Set<String> result = new HashSet<String>();
		List<Thread> list = new ArrayList<Thread>();
		
		if (through_paxos){
			// Request go through paxos, uncoordinated
			JSONObject reqVal = findRequest(query);			
						
			int idx = rand.nextInt(numReplica);
			// send uncoordinated requests to all the groups
			for ( int i=0; i<numPartition; i++) {
				String serviceName = allServiceNames.get(i);
				List<InetSocketAddress> l = allGroups.get(i);
				InetSocketAddress addr = l.get(idx);
				AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
				
				// do not need coordination
				request.setNeedsCoordination(false);
								
				//FIXME: send out request in parallel, then get back all the responses			 
				Runnable runnable = () -> {
					    try {
					    	AppRequest response =  (AppRequest) this.sendRequest(request, addr);
					    	result.add(response.getValue());
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					};
				Thread thread = new Thread(runnable);
				thread.start();
				
				list.add(thread);
			}
			
			
		} else {
			// send request directly to the database
			int idx = rand.nextInt(numReplica);
			for ( int i=0; i<numPartition; i++) {
				MongoCollection<Document> collection = allCollections.get(i*numReplica+idx);
				Runnable runnable = () -> {
					MongoCursor<Document> cursor = collection.find(query).iterator(); 							
					try {
					    while (cursor.hasNext()) {
					    	// This is a string
					        // cursor.next().toJson();
					    	String record = cursor.next().toJson();
					        result.add(record);
					    }
					} finally {
					    cursor.close();
					}
				};
				Thread thread = new Thread(runnable);
				thread.start();
				
				list.add(thread);
			}
		}
		
		for (Thread thread: list){
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	/**
	 * This only works for consistent hash scheme
	 * @param query
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Set<String> sendFindRequest(BasicDBObject query) {
		Set<String> result = new HashSet<String>();
		
		if(through_paxos){ 
			JSONObject reqVal = findRequest(query);			
			
			int idx = rand.nextInt(numReplica);
			// send uncoordinated requests to all the groups
			for ( int i=0; i<numReplica; i++) {
				String serviceName = allServiceNames.get(i);
				List<InetSocketAddress> l = allGroups.get(i);
				InetSocketAddress addr = l.get(idx);
				AppRequest request = new AppRequest(serviceName, reqVal.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
				request.setNeedsCoordination(false);
				// do not need coordination
				try {
					
					AppRequest response =  (AppRequest) this.sendRequest(request, addr);
					result.add(response.getValue());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			// send request directly to the database
			int idx = rand.nextInt(numReplica);
			
			for ( int i=0; i<numReplica; i++ ) {
				MongoCollection<Document> collection = allCollections.get(i*numReplica+idx);
				MongoCursor<Document> cursor = collection.find(query).iterator(); 							
				try {
				    while (cursor.hasNext()) {
				    	// This is a string
				        // cursor.next().toJson();
				    	String record = cursor.next().toJson();
				        result.add(record);
				    }
				} finally {
				    cursor.close();
				}
			}
		}
		
		// aggregate results from all replicas
		return result;
		
	}
	
	protected static JSONObject restoreRequest(String cmd) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.RESTORE_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), cmd);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return json;
	}
	
	protected static JSONObject indexRequest(JSONArray arr) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.CREATE_INDEX_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), arr.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;
	}
	
	protected static JSONObject findRequest(BasicDBObject query) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.FIND_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), query);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;		
	}
	
	protected static JSONObject findRequest(BasicDBObject query, int batch) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.FIND_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), query);
			json.put(MongoApp.KEYS.PARAM.toString(), batch);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;	
	}
	
	protected static JSONObject insertRequest(Document doc) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.INSERT_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), doc);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;
	}
	
	protected static JSONObject replaceRequest(Document oldVal, Document newVal) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.REPLACE_ONE_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), oldVal);
			json.put(MongoApp.KEYS.PARAM.toString(), newVal);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;
	}
	
	protected static JSONObject deleteOneRequest(Document doc) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.DELETE_ONE_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), doc);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;
	}
	
	protected static JSONObject deleteManyRequest(Document doc) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.DELETE_MANY_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), doc);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return json;
	}
	
	protected static JSONObject dropRequest(Document doc) {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.DROP_OP.toString());
			json.put(MongoApp.KEYS.QUERY.toString(), doc);
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return json;
	}
	
	/**
	 * Generate a random String with the fixed length
	 * @return
	 */
	protected static String getRandomKey(int l) {
		StringBuilder salt = new StringBuilder();
		while (salt.length() < key_length) { // length of the random string.
			int index = (int) (rand.nextFloat() * SALTCHARS.length());
			salt.append(SALTCHARS.charAt(index));
		}
		String saltStr = salt.toString();
		return saltStr;
	}
	
	// For experiment use only
	private static int sent = 0;
	private static synchronized void incrSent(int num){
		sent++;
	}
	
	private static int received = 0;
	private static synchronized void incrRcvd() {
		received ++; 
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@SuppressWarnings({ "unchecked" })
	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("Client starts");
		MongoAppClient client = new MongoAppClient();
		createAllGroups(client, true);
		
		String testServiceName = PaxosConfig.getDefaultServiceName();
				
		Map<String, InetSocketAddress> servers = PaxosConfig.getActives();		
		
		Set<InetSocketAddress> initGroup = new HashSet<InetSocketAddress>();
		for(String name: servers.keySet()){
			System.out.println(name+" "+servers.get(name));
			initGroup.add(servers.get(name));
		}
		
		incrSent(1);
		client.sendRequest(new CreateServiceName(testServiceName, 
				"", initGroup), 
				new RequestCallback() {
					long createTime = System.currentTimeMillis();
					@Override
					public void handleResponse(Request response) {
						System.out
						.println("Response to create service name [test"
								+ "] = "
								+ (response)
								+ " received in "
								+ (System.currentTimeMillis() - createTime)
								+ "ms");
						incrRcvd();
					}
				}
			);
		
		while(sent < received) {
			Thread.sleep(500);
		}

		int numToSend = 1;
		// insert
		List<Document> list = new ArrayList<Document>();
		List<String> keys = new ArrayList<String>();
		for (int i=0; i<numToSend; i++) {
			Document bson = new Document();
			String key = getRandomKey(key_length);
			bson.put(MongoApp.KEYS.KEY.toString(), key);
			// set price, price is a test field
			bson.put("price", i);
			list.add(bson);
			keys.add(key);
		}
		
		for (int i=0; i<numToSend; i++) {
			JSONObject json = insertRequest(list.get(i));
			incrSent(1);
			client.sendRequest(new AppRequest(testServiceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),					
					new RequestCallback() {
				long createTime = System.currentTimeMillis();
				@Override
				public void handleResponse(Request response) {
					System.out
							.println("Response for request ["
									+ json
									+ "] = "
									+ (response)
									+ " received in "
									+ (System.currentTimeMillis() - createTime)
									+ "ms");
					incrRcvd();
				}
			});			
		}
		Thread.sleep(500);
		
		for (int i=0; i<numToSend; i++) {
			Document bson = new Document();
			String key = keys.get(i);
			bson.put(MongoApp.KEYS.KEY.toString(), key);
			bson.put("price", i+10);
			JSONObject json = replaceRequest(list.get(i), bson);
			incrSent(1);
			client.sendRequest(new AppRequest(testServiceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false),
					new RequestCallback() {
				long createTime = System.currentTimeMillis();
				@Override
				public void handleResponse(Request response) {
					System.out
							.println("Response for request ["
									+ json
									+ "] = "
									+ (response)
									+ " received in "
									+ (System.currentTimeMillis() - createTime)
									+ "ms");
					incrRcvd();
				}
			});
		}
		Thread.sleep(500);
		
		BasicDBObject query = new BasicDBObject("price", 
                new BasicDBObject("$gt", 5));
		
		JSONObject json = findRequest(query);
		incrSent(1);
		client.sendRequest(new AppRequest(testServiceName, json.toString(), AppRequest.PacketType.DEFAULT_APP_REQUEST, false), 
				new RequestCallback() {
			long createTime = System.currentTimeMillis();
			@Override
			public void handleResponse(Request response) {
				System.out
						.println("Response for request ["
								+ json
								+ "] = "
								+ (response)
								+ " received in "
								+ (System.currentTimeMillis() - createTime)
								+ "ms");
				System.out.println("Request type:"+response.getRequestType());
				incrRcvd();
			}
		});
		
		
		while(received < sent) {
			Thread.sleep(500);
		}
		
		Set<String> result = client.sendFindRequest(query); 
		System.out.println("The search result is:"+result);
		/*
		JSONObject json = new JSONObject();
		BasicDBObject query = new BasicDBObject("price", 
                new BasicDBObject("$gt", 30000));
		
		json.put(MongoApp.KEYS.TYPE.toString(), MongoApp.FIND_OP.toString());
		json.put(MongoApp.KEYS.QUERY.toString(), query);
		final String requestValue = json.toString();
		
		System.out.println("Send request ["+requestValue+"] to "+PaxosConfig.getDefaultServiceName());
		client.sendRequest(PaxosConfig.getDefaultServiceName(),
				requestValue, 
				new RequestCallback() {
					long createTime = System.currentTimeMillis();
					@Override
					public void handleResponse(Request response) {
						System.out
								.println("Response for request ["
										+ requestValue
										+ "] = "
										+ (response instanceof ClientRequest ? ((ClientRequest) response)
												.getResponse() : null)
										+ " received in "
										+ (System.currentTimeMillis() - createTime)
										+ "ms");
					}
				});
		Thread.sleep(100);
		*/
		client.close();
	}

}
