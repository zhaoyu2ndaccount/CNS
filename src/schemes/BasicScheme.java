package schemes;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.BasicDBObject;

import interfaces.PartitionScheme;

/**
 * @author gaozy
 *
 */
public class BasicScheme implements PartitionScheme{
	
	protected final int numPartitions;
	protected final int numReplicas;
	
	/**
	 * @param numServiceNames
	 * @param numReplicas
	 */
	public BasicScheme(int numServiceNames, int numReplicas) { 
		this.numPartitions = numServiceNames;
		this.numReplicas = numReplicas;
	}
	
	@Override
	public List<InetSocketAddress> getSingleReplicaActives(BasicDBObject bson, Map<Integer, List<InetSocketAddress>> map){
		throw new RuntimeException("unimplemented");
	}
	
	@Override
	public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map) {
		throw new RuntimeException("unimplemented");
	}

	@Override
	public Map<Integer, List<InetSocketAddress>> getGroupForAllServiceNames(int numPartition, int numReplica,
			Map<String, InetSocketAddress> actives) {
		throw new RuntimeException("unimplemented");
	}
	
	@Override
	public String getServiceName(Document bson, Map<Integer, String> map){
		throw new RuntimeException("unimplemented");
	}

}
