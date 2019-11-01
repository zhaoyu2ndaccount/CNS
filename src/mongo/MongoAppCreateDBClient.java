package mongo;

import java.io.IOException;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

/**
 * @author gaozy
 *
 */
public class MongoAppCreateDBClient {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		MongoAppClient client = new MongoAppClient();
		
		MongoAppClient.createAllGroups(client, false);
		
		Map<Integer, MongoCollection<Document>> collections = client.allCollections;
		
		System.out.println(collections);
		
		
	}

}
