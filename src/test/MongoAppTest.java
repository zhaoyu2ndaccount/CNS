package test;


import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * @author gaozy
 *
 */
public class MongoAppTest {
	private static final String DB_NAME = "test";
	private static final String TABLE_NAME = "active";
	
	/**
	 * @param args
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws JSONException {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		MongoDatabase database = mongoClient.getDatabase(DB_NAME);
		MongoCollection collection = database.getCollection(TABLE_NAME);	
		
		JSONObject json = new JSONObject();
		json.put("price", 1);
		Document bson = new Document();
		bson.put("price", 5);
		BasicDBObject query = new BasicDBObject("price", 
                new BasicDBObject("$gt", 2));
		
		// insert
		collection.insertOne(Document.parse(json.toString()));
		json.put("price", 2);
		collection.insertOne(Document.parse(json.toString()));
		json.put("price", 3);
		collection.insertOne(Document.parse(json.toString()));
		
		// update
		UpdateResult result = collection.replaceOne(Document.parse(json.toString()), bson);  //findAndUpdate(Document.parse(json.toString()), bson);
		System.out.println(result+":"+result.getMatchedCount()+ " " + result.wasAcknowledged()+" "+result.getModifiedCount());
		
		// find
		MongoCursor<Document> cursor = collection.find(query).iterator(); 
		try {
		    while (cursor.hasNext()) {
		    	// This is a string
		        System.out.println(cursor.next().toJson());
		    }
		} finally {
		    cursor.close();
		}
		
		// delete
		DeleteResult res = collection.deleteOne(query);
		System.out.println(res.acknowledged(1)+" "+res.getDeletedCount()+ " "+res.wasAcknowledged()+" "+res.unacknowledged());
		
		collection.drop();
		
		collection.insertOne(Document.parse(json.toString()));
		
		query = new BasicDBObject("price", 
                new BasicDBObject("$gt", 1));
		// find
		cursor = collection.find(query).iterator(); 

		try {
		    while (cursor.hasNext()) {
		    	// This is a string
		        System.out.println(cursor.next().toJson());
		    }
		} catch (Exception e) {
			e.printStackTrace();
		} finally {			
		    cursor.close();
		}
		
		
		
		// clearup
//		mongoClient.dropDatabase(DB_NAME);
	}
}
