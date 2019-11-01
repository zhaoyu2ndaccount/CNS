package mongo;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Constants for MongoApp
 * @author gaozy
 *
 */
public class MongoApp {
	
	/**
	 * @author gaozy
	 *
	 */
	public enum KEYS
	{
		/**
		 * The name of key in each record
		 */
		KEY,
		/**
		 * The type of a query: find, update, insert
		 */
		TYPE,
		/**
		 * The content of a query: specified by client
		 */
		QUERY,
		/**
		 * Some additional parameter needed for some operations like update
		 */
		PARAM,
		/**
		 * The code in the response
		 */
		CODE,
		/**
		 * The string used to set table name through an environment variable
		 */
		TABLE;
	}
	
	/**
	 * The code to indicate whether a query has been executed successfully
	 * @author gaozy
	 *
	 */
	public enum code
	{
		/**
		 * 
		 */
		error,
		/**
		 * 
		 */
		success;
	}
	
	/**
	 * The list of operations supported by this MongoApp
	 * @author gaozy
	 *
	 */
	final protected static String FIND_OP = "find";
	/**
	 * Get operation
	 */
	final protected static String GET_OP = "get";
	// this is equivelent to update operation
	final protected static String REPLACE_ONE_OP = "replace";
	final protected static String INSERT_OP = "insert";
	final protected static String DELETE_ONE_OP = "delete_one";
	final protected static String DELETE_MANY_OP = "delete_many";
	final protected static String DROP_OP = "drop";
	final protected static String CREATE_INDEX_OP = "create_index";
	final protected static String RESTORE_OP = "restore";
	final protected static String PING = "ping";
	
	
	// 10M 
	final static int num_records = 10000000;
	// 10 attributes
	public final static int num_attributes = 12;
	// prefix
	public final static String ATTR_PREFIX = "a";
	// experiment wait time: 30s
	final static long EXP_WAIT_TIME = 40*1000;
	// total number of requests to send
	final static int TOTAL_REQS = 400000;
	
	final static long EXP_TIME = 2*60*1000;
	
	// probe related parameters
	final static int PROBE_RUN_DURATION = 10;
	final static double PROBE_RESPONSE_THRESHOLD = 0.9;
	final static double PROBE_LOAD_INCREASE_FACTOR = 1.1;
	final static double PROBE_LATENCY_THRESHOLD = 10000;
	final static int MAX_CONSECURIVE_FAILURES = 3;
	final static int MAX_RUN_ATTEMPTS = 200;
	
	/**
	 * max value of each attribute
	 */
	public final static int MAX_VALUE = 100000;
	// max number for batch in search query
	final static int MAX_BATCH_NUM = 200;

	// FIXME: used to run dimnensionality experiment, need to refine later
	public static int MAX_INT_VALUE = Integer.MAX_VALUE;
	public static int NUM_BUCKETS = 4;
	public static int INTERVAL = MAX_INT_VALUE / NUM_BUCKETS;

	/**
	 * @param data
	 * @return a byte stream
	 * @throws IOException
	 */
	public static byte[] compress(String data) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length());
		GZIPOutputStream gzip = new GZIPOutputStream(bos);
		gzip.write(data.getBytes());
		gzip.close();
		byte[] compressed = bos.toByteArray();
		bos.close();
		return compressed;
	}
	
	/**
	 * @param compressed
	 * @return a string decompressed from the byte stream
	 * @throws IOException
	 */
	public static String decompress(byte[] compressed) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
		GZIPInputStream gis = new GZIPInputStream(bis);
		BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
		StringBuilder sb = new StringBuilder();
		String line;
		while((line = br.readLine()) != null) {
			sb.append(line);
		}
		br.close();
		gis.close();
		bis.close();
		return sb.toString();
	}
}
