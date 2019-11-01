package schemes;

import com.mongodb.BasicDBObject;
import mongo.MongoApp;
import org.bson.Document;

import java.net.InetSocketAddress;
import java.util.*;


// TODO: use a consistent hashing to implement this class
public class ConsistentHashing extends BasicScheme {

    /**
     * @param numServiceNames
     * @param numReplicas
     */
    public ConsistentHashing(int numServiceNames, int numReplicas) {
        super(numServiceNames, numReplicas);
    }

    @Override
    public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map) {
        assert(bson.containsKey(MongoApp.KEYS.KEY.toString()));
        String key = bson.getString(MongoApp.KEYS.KEY.toString());
        // byte[] b = key.getBytes();
        // int retval = b.hashCode();

        return null;
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

        return null;
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

    }

}
