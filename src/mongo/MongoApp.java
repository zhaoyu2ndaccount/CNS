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
	// this is equivelent to update operation
	final protected static String REPLACE_ONE_OP = "replace";
	final protected static String INSERT_OP = "insert";
	final protected static String DELETE_ONE_OP = "delete_one";
	final protected static String DELETE_MANY_OP = "delete_many";
	final protected static String DROP_OP = "drop";
	final protected static String CREATE_INDEX_OP = "create_index";
	final protected static String RESTORE_OP = "restore";
	
	
	// 100K 
	final static int num_records = 1000000;
	// 10 attributes
	final static int num_attributes = 10;
	// prefix
	final static String ATTR_PREFIX = "a";
	// experiment time
	final static long EXP_TIME = 2*60*1000;
	// max value 
	final static int MAX_VALUE = 100000;
	
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
