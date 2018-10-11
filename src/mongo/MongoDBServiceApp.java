package mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.examples.PaxosAppRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.noopsimple.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;


/**
 * A mongo DB wrapper
 * 
 * There is only one database being used by all active replicas.
 * Each active replica corresponds to a table.
 * The table name is specified through an environment variable.
 * 
 * @author gaozy
 *
 */
public class MongoDBServiceApp implements Replicable {
	private final String TABLE_NAME;
	protected final static String DB_NAME = "active"; /* "active" by default */
	private final String ERROR_MESSAGE;
	private final String SUCCESS_MESSAGE;
	
	
	private MongoClient mongoClient;
	private MongoCollection collection;
	
	/**
	 * 
	 */
	public MongoDBServiceApp() {
		JSONObject json = new JSONObject();
		try {
			json.put(MongoApp.KEYS.CODE.toString(), MongoApp.code.error.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ERROR_MESSAGE = json.toString();
		
		try {
			json.put(MongoApp.KEYS.CODE.toString(), MongoApp.code.success.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SUCCESS_MESSAGE = json.toString();
		
		if (System.getProperty(MongoApp.KEYS.TABLE.toString()) != null) {
			TABLE_NAME = System.getProperty(MongoApp.KEYS.TABLE.toString());
		} else {
			TABLE_NAME = PaxosConfig.getDefaultServiceName();
		}
		
		int port = 27017;
		if (System.getProperty("port") != null) {
			port = Integer.parseInt(System.getProperty("port"));
		}
		this.mongoClient = new MongoClient("localhost", port);
		
		MongoDatabase database = this.mongoClient.getDatabase(DB_NAME);
		this.collection = database.getCollection(TABLE_NAME);				
	}
	
	@Override
	public boolean execute(Request request) {
		// execute request here
//		System.out.println("Received request:"+request);
		
		if (request instanceof AppRequest) {
			AppRequest req = ((AppRequest) request);
			try {
				JSONObject value = new JSONObject(req.getValue());
//				System.out.println("Value is:"+value);
				String type = value.getString(MongoApp.KEYS.TYPE.toString());
//				System.out.println("Type is:"+type);
				String query = value.getString(MongoApp.KEYS.QUERY.toString());
//				System.out.println("Query is:"+query);
				
				switch(type){
					case MongoApp.FIND_OP:{
							JSONArray array = new JSONArray();
						
							Integer limit = null;
							// limit the number of returned records
							if (value.has(MongoApp.KEYS.PARAM.toString()))
								limit = value.getInt(MongoApp.KEYS.PARAM.toString());
							
							MongoCursor<Document> cursor = (limit==null) ? this.collection.find(Document.parse(query)).iterator(): 
								this.collection.find(Document.parse(query)).limit(limit).iterator();
							
							int count = 0;				
							try {
							    while (cursor.hasNext()) {
							    	// This is a string
							        // cursor.next().toJson();
							    	String record = cursor.next().toJson();
							        array.put(record);
							        count++;
							    }
							} finally {
							    cursor.close();
							}
							// System.out.println("Result set size:"+count);
							
							String resp = array.toString();
						
							// set response
							req.setResponse(resp);
						}
						break;
					case MongoApp.REPLACE_ONE_OP:{
//							System.out.println("This is an update request");
							
							// To update, we need to get another parameter
							String param = value.getString(MongoApp.KEYS.PARAM.toString());
							UpdateResult result = this.collection.replaceOne(Document.parse(query), Document.parse(param));
							if (result.wasAcknowledged() && result.getMatchedCount() == 1 && result.getModifiedCount() == 1) {
								req.setResponse(SUCCESS_MESSAGE);
							} else {
								req.setResponse(ERROR_MESSAGE);
							}
						}						
						break;
					case MongoApp.INSERT_OP:{
//							System.out.println("This is an insert request");
							// convert JSONObject to Document
							// Document doc = new Document((Map<String, Object>) query);
							this.collection.insertOne(Document.parse(query));
						}
						req.setResponse(SUCCESS_MESSAGE);
						break;
					case MongoApp.DELETE_ONE_OP:
						DeleteResult res = collection.deleteOne(Document.parse(query));
						
						if (res.wasAcknowledged() && res.getDeletedCount() == 1) {
							req.setResponse(SUCCESS_MESSAGE);
						} else {
							req.setResponse(ERROR_MESSAGE);
						}
						break;
					case MongoApp.DELETE_MANY_OP:
						DeleteResult resMany = collection.deleteMany(Document.parse(query));
						if (resMany.wasAcknowledged() && resMany.getDeletedCount() >= 1) {
							req.setResponse(SUCCESS_MESSAGE);
						} else {
							req.setResponse(ERROR_MESSAGE);
						}
						break;
					case MongoApp.DROP_OP:{
						/*
						String cmd = "mongo "+ DB_NAME + " --eval 'db.dropDatabase()'";
						try {
							Process p = Runtime.getRuntime().exec(cmd);
							p.waitFor();
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
						*/
						collection.drop();
						req.setResponse(SUCCESS_MESSAGE);
						}	
						break;
					case MongoApp.CREATE_INDEX_OP:
//						System.out.println(">>>>>>>> Ready to create indexes:"+query);
						JSONArray arr = new JSONArray(query);
						List<IndexModel> list = new ArrayList<IndexModel>();
						for (int i=0; i<arr.length(); i++) {
							list.add(new IndexModel( Document.parse(arr.get(i).toString()) ) );
						}
						
						collection.createIndexes(list);
//						System.out.println("Indexes created successfully!");
						req.setResponse(SUCCESS_MESSAGE);
						break;
					case MongoApp.RESTORE_OP:
						// FIXME: different instance execute differently as the table name might be different
						System.out.println(">>>>>>>>>> Ready to restore a collection:"+query);
						// query is the path to the dumped DB
						String cmd = "mongorestore -d active -c "+ TABLE_NAME + " "+query;
						
						// ProcessBuilder proc = new ProcessBuilder(cmd);
						try {
							Process p = Runtime.getRuntime().exec(cmd);
							p.waitFor();
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
						System.out.println(">>>>>>>>>> DB restore is done!");
						req.setResponse(SUCCESS_MESSAGE);
						break;
					default:
						// unsupported query
						req.setResponse(ERROR_MESSAGE);
						break;
				}				
				
			} catch (JSONException e) {
				e.printStackTrace();
				// Invalid request, set an error code in response
				req.setResponse(ERROR_MESSAGE);
			}
		} else {
			((PaxosAppRequest) request).setResponse(ERROR_MESSAGE);
		}
		
		return true;
	}

	@Override
	public Request getRequest(String stringified) throws RequestParseException {
		try {
			return NoopApp.staticGetRequest(stringified);
		} catch (RequestParseException | JSONException e) {
			// do nothing by design
		}
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return NoopApp.staticGetRequestTypes();
	}

	@Override
	public String checkpoint(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		// execute request without replying back to client
		
		// identical to above unless app manages its own messaging
		return this.execute(request);
	}

	@Override
	public boolean restore(String name, String state) {
		// TODO Auto-generated method stub
		return true;
	}

}
