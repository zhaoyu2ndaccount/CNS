package interfaces;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;

/**
 * @author gaozy
 *
 */
public interface PartitionScheme {	
	/**
	 * Based on the entry to determine a list of service names
	 * @param bson 
	 * @param map
	 * @return a GigaPaxos service name
	 */
	public String getServiceName(Document bson, Map<Integer, String> map);

	/**
	 *
	 * @param bson 
	 * @param map 
	 * @return all the actives in a single replica for search query
	 */
	public List<InetSocketAddress> getSingleReplicaActives(BasicDBObject bson, Map<Integer, List<InetSocketAddress>> map);
	
	/**
	 * @param bson
	 * @param map
	 * @return a group of active replicas
	 */
	public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map);
	
	/**
	 * 
	 * @param numPartition 
	 * @param numReplica 
	 * @param actives 
	 * @return a map between service names and sets of Paxos groups
	 */
	public Map<Integer, List<InetSocketAddress>> getGroupForAllServiceNames(int numPartition, 
			int numReplica, Map<String, InetSocketAddress> actives);
	
	

}
