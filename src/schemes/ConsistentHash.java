package schemes;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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
public class ConsistentHash extends  BasicScheme {
	
	final int numServiceNames;
	
	/**
	 * @param numPartitions 
	 * @param numReplicas
	 */
	public ConsistentHash(int numPartitions, int numReplicas) {
		super(numPartitions, numReplicas);
		this.numServiceNames = numReplicas;
//		serviceNamePrefix = Config.getGlobalString(TC.TEST_GUID_PREFIX);
//		System.out.println("Service Name Prefix is:"+serviceNamePrefix);
	}

	@Override
	public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map) {
		assert(bson.containsKey(MongoApp.KEYS.KEY.toString()));
		String key = bson.getString(MongoApp.KEYS.KEY.toString());
		byte[] b = key.getBytes();
		int retval = b.hashCode();
		
		assert(map.keySet().size() >= this.numServiceNames);
		
		return map.get(retval % this.numServiceNames);
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
		
		return map.get(retval % this.numServiceNames);
	}
	
	@Override
	public Map<Integer, List<InetSocketAddress>> getGroupForAllServiceNames(int numPartition, int numReplica,
			Map<String, InetSocketAddress> actives) {
		Map<Integer, InetSocketAddress> servers = new HashMap<Integer, InetSocketAddress>();
		int idx = 0;
		for (String name: actives.keySet()){
			System.out.println("Active:"+name+","+actives.get(name));
			servers.put(idx, actives.get(name));
			++idx;
		}
		
		int numGroups = numPartition;
		
		Map<Integer, List<InetSocketAddress>> map = new HashMap<Integer, List<InetSocketAddress>>();
		for (int i=0; i<numGroups; i++){
			List<InetSocketAddress> replica = new ArrayList<InetSocketAddress>();
			for (int j:servers.keySet()) {
				if(j%numGroups == i){
					replica.add(servers.get(j));
				}
			}
			map.put(i, replica);
		}
		
		return map;
	}
	
}
