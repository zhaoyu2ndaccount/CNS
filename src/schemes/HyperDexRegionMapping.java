package schemes;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.BasicDBObject;
import mongo.MongoApp;
import org.bson.Document;
import schema.Schema;

import java.net.InetSocketAddress;
import java.util.*;


/**
 * HyperDex Region Mapping scheme without keyspace
 * The key-based update and lookup need to be sent out
 * to all shards.
 */
public class HyperDexRegionMapping extends BasicScheme {

    List<String> attributes;

    int interval = MongoApp.INTERVAL;
    int num_buckets = MongoApp.NUM_BUCKETS;

    private HashFunction hf = Hashing.md5();

    /**
     * @param numPartitions
     * @param numReplicas
     */
    public HyperDexRegionMapping(int numPartitions, int numReplicas) {
        super(numPartitions, numReplicas);
        this.attributes = Schema.attributes;
        System.out.println("attributes:" + attributes);
    }

    /**
     *
     * @param values
     * @return the id of the region based on list of
     */
    private String generateRegionIdFromValue(List<String> values){
        return  String.join("-", values);
    }

    /**
     * @param query
     * @return the set of nodes to query
     */
    private Set<Integer> getNodesFromQuery(BasicDBObject query){

        List<String> regions = new ArrayList<>();
        regions.add("");

        for (int i=0; i<attributes.size(); i++){
            String attr = attributes.get(i);
            List<String> new_regions = new ArrayList<>();

            BasicDBObject range = (BasicDBObject) query.getOrDefault(attr, null);

            // System.out.println(i+"-th attribute: "+attr+", range:"+range);

            int lower_bound = 0;
            int upper_bound = num_buckets;

            if (range != null){
                lower_bound = range.getInt("$gte")/interval;
                upper_bound = range.getInt("$lt")/interval;
            }

            for (int k=lower_bound; k<upper_bound+1; k++){
                for (String r : regions) {
                    new_regions.add(r+k+"-");
                }
            }

            regions = new_regions;
        }

        Set<Integer> retval = new HashSet<>();
        for (String r : regions){
            retval.add(Hashing.consistentHash(hf.hashBytes(r.substring(0, r.length()-1).getBytes()), super.numPartitions));
        }

        return retval;
    }

    @Override
    public List<InetSocketAddress> getServiceGroup(Document bson, Map<Integer, List<InetSocketAddress>> map) {
        assert(bson.containsKey(MongoApp.KEYS.QUERY.toString()));
        Document query = (Document) bson.get(MongoApp.KEYS.QUERY.toString());



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

    @Override
    public List<Integer> getPartitionsForSearch(BasicDBObject query) {
        // System.out.println(">>>>>>>>> Query:"+query);

        Set<Integer> result = getNodesFromQuery( query );

        // System.out.println(">>>>>>>>> Result:" + result);
        return new ArrayList<>( result );

    }

    /**
     * @param args
     */
    public static void main(String[] args) {

    }

}
