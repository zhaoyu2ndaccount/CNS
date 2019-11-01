package util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ConsistentHash {

    private final static int numBuckets = 5;

    public static void main(String[] args){
        ArrayList<String> al = new ArrayList<String>();
        al.add("redis1");
        al.add("redis2");
        al.add("redis3");
        al.add("redis4");

        String[] userIds =
                {"-84942321036308",
                        "-76029520310209",
                        "-68343931116147",
                        "-54921760962352"
                };
        HashFunction hf = Hashing.md5();

        for (int i=0; i<userIds.length; i++) {
            int retval = Hashing.consistentHash(hf.hashBytes(userIds[i].getBytes()), numBuckets);
            System.out.println(retval);
        }
    }

}