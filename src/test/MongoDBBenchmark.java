package test;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import mongo.MongoApp;

/**
 * @author gaozy
 *
 */
public class MongoDBBenchmark {
	
	private static final DecimalFormat df1 = new DecimalFormat( "#.######" );
	private static final DecimalFormat df2 = new DecimalFormat( "#.##" );
	private static int num_records = 100;
	private final static int num_attributes = 1;
	private final static int max_val = MongoApp.MAX_VALUE;
	
	private final static int total_req = 10000;
	private final static String ATTR_PREFIX = "a";
	
	private final static Random rand = new Random();
	
	static private double fraction = 0.00001;
	
	private static boolean toInsert = false;
	private static boolean toClean = false;
	
	static int rcvd = 0;
	static synchronized void incr() {
		rcvd++;
	}
	
	static CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<Integer>();
	static CopyOnWriteArrayList<Double> latency = new CopyOnWriteArrayList<Double>();
	static CopyOnWriteArrayList<Double> traverse_lat = new CopyOnWriteArrayList<Double>();
	
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		if(args.length == 0){
			System.out.println("Please provide the host name and collection name.");
			System.exit(0);
		}
		
		if (System.getProperty("num_records") != null) {
			num_records = Integer.parseInt(System.getProperty("num_records"));
		}		

		if (System.getProperty("insert") != null) {
			toInsert = Boolean.parseBoolean(System.getProperty("insert"));
		}
		
		if (System.getProperty("clean") != null) {
			toClean = Boolean.parseBoolean(System.getProperty("clean"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
			
		String collection_name = args[1]; //"test_"+num_records/10000;
		
		String host = args[0];
		int port = 27017;
		
		MongoClient mongoClient = new MongoClient(host, port);
		
		MongoDatabase database = mongoClient.getDatabase("active");
		MongoCollection collection = database.getCollection(collection_name);
		
		ExecutorService executor = Executors.newFixedThreadPool(100);
		
		if(toInsert) {
			double interval = ((double) max_val )/num_records;
			
			// insert records
			for(int i=0; i<num_records; i++) {
				Document bson = new Document();
				for (int k=0; k<num_attributes; k++) {
					bson.put(ATTR_PREFIX+k, new Double(df1.format(i*interval)));
				}
				collection.insertOne(bson);
			}
			System.out.println("All "+num_records+" have been inserted successfully!");
			
			for (int k=0; k<num_attributes; k++) {
				Document doc = new Document();
				// incremental order
				doc.put(ATTR_PREFIX+k, 1);
				collection.createIndex(doc);
			}
		}
		
		
		long begin = System.currentTimeMillis();
		int count = 0; 
		while(count < total_req) {
			// executor.execute(new QueryRunnable(collection));
			
			BasicDBObject query = new BasicDBObject();
			double drift = max_val*fraction;
			BasicDBObject bson = new BasicDBObject();
			double start = rand.nextDouble()+rand.nextInt(max_val);
			double end = start + drift;
			bson.put("$gte", start);
			bson.put("$lt", end);
			query.put(ATTR_PREFIX+rand.nextInt(num_attributes), bson);
			// System.out.println("QUERY:"+query);
			
			long t1 = System.nanoTime();
//			collection.find(query)
			MongoCursor<Document> cursor = collection.find(query).batchSize(1000).iterator(); 
			int cnt = 0;
			long t2 = System.nanoTime();
			try {
			    while (cursor.hasNext()) {
			    	// This is a string
			    	cursor.next();
			    	cnt++;
			    }
			} finally {
			    cursor.close();
			}
			long elapsed = System.nanoTime() - t2;
			traverse_lat.add(elapsed/1000.0);
			elapsed = System.nanoTime() - t1;
			System.out.println("Total:"+cnt+",elapsed:"+elapsed+"us");
			incr();
			list.add(cnt);
			latency.add(elapsed/1000.0);
			
			count++;
		}
		
		while(rcvd < total_req) {
			System.out.println("Received:"+rcvd);
			Thread.sleep(1000);
		}
		
		// double elapsed = (System.currentTimeMillis()-begin)/1000.0;
		// System.out.println("Elapsed: "+elapsed);
		// System.out.println("Thruput: "+rcvd/elapsed);
		
		try {
		    // System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		    executor.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
		    System.err.println("tasks interrupted");
		}
		finally {
		    if (!executor.isTerminated()) {
		        System.err.println("cancel non-finished tasks");
		    }
		    executor.shutdownNow();
		    // System.out.println("shutdown finished");
		}
		
		double l = 0;
		for (double lat : latency){
			l += lat;
		}
		l = l/total_req;
		double d=0;
		for (double lat: latency) {
			d = d+ (lat-l)*(lat-l);
		}
		Collections.sort(latency);
		System.out.println("The average latency is:"+df2.format(l)+"us, std:"+df2.format(Math.sqrt(d)/total_req)+"us, median:"+latency.get(latency.size()/2));
			
		double ave = 0;
		for (double lat:traverse_lat){
			ave += lat;
		}
		System.out.println("Traverse latency is "+df2.format(ave/total_req)+"us");
		
		int num_records = 0;
		for (int r:list){
			num_records += r;
		}
		System.out.println("Average resultset size is:"+num_records/total_req);
		
		if(toClean)
			collection.drop();
		
		mongoClient.close();
	}
}
