package schemes;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;

import com.mongodb.BasicDBObject;

import mongo.MongoApp;

/**
 * A consistent hash scheme that assigns each record based on a consistent hash function
 * @author gaozy
 *
 */
public class DeterministicHashing extends BasicScheme {
	
	// final int numServiceNames;
	
	/**
	 * @param numPartitions 
	 * @param numReplicas
	 */
	public DeterministicHashing(int numPartitions, int numReplicas) {
		super(numPartitions, numReplicas);
	}

	@Override
	public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map) {
		assert(bson.containsKey(MongoApp.KEYS.KEY.toString()));
		String key = bson.getString(MongoApp.KEYS.KEY.toString());
		byte[] b = key.getBytes();
		int retval = b.hashCode();
		
		assert(map.keySet().size() >= super.numPartitions);
		
		return map.get(retval % super.numPartitions);
	}
	
	@Override
	public List<InetSocketAddress> getSingleReplicaActives(BasicDBObject query, Map<Integer, List<InetSocketAddress>> map){
		// unrelated to the search query
		Random rand = new Random();
		int idx = rand.nextInt() % this.numReplicas;
		
		List<InetSocketAddress> actives = new ArrayList<InetSocketAddress>();
		for (int group : map.keySet()) {
			actives.add(map.get(group).get(idx));
		}
		
		return actives;
	}
	
	
	@Override
	public String getServiceName(Document bson, Map<Integer, String> map){
		String key = bson.getString(MongoApp.KEYS.KEY.toString());
		byte[] b = key.getBytes();
		int retval = b.hashCode();
		
		return map.get(retval % super.numPartitions);
	}
	
	@Override
	public Map<Integer, List<InetSocketAddress>> getGroupForAllServiceNames(int numPartition, int numReplica,
			Map<String, InetSocketAddress> actives) {
		Map<Integer, InetSocketAddress> servers = new HashMap<Integer, InetSocketAddress>();
		int idx = 0;
		List<String> names = new ArrayList<String>(actives.keySet());
		Collections.sort(names);
		for ( int i=0; i<names.size(); i++){
			String name = names.get(i);
			System.out.println("Active:"+name+","+actives.get(name));
			servers.put(idx, actives.get(name));
			++idx;
		}
		
		Map<Integer, List<InetSocketAddress>> map = new HashMap<Integer, List<InetSocketAddress>>();
		for (int i=0; i<numPartition; i++){
			List<InetSocketAddress> replica = new ArrayList<InetSocketAddress>();
			for (int j:servers.keySet()) {
				if(j%numPartition == i){
					replica.add(servers.get(j));
				}
			}
			map.put(i, replica);
		}
		
		return map;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		for (int i=4; i<6; i++) {
			Map<String, InetSocketAddress> actives = new HashMap<>();
			for (int j=1; j<i*i+1; j++) {
				InetSocketAddress addr = new InetSocketAddress("10.0.0."+j, 3000);
				actives.put("node-"+j, addr);
			}
			DeterministicHashing scheme = new DeterministicHashing(i, i);
			System.out.println(i+" : "+scheme.getGroupForAllServiceNames(i, i, actives));
		}
	}
}
