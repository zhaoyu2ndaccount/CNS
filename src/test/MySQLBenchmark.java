package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author gaozy
 *
 */
public class MySQLBenchmark {
	private static final DecimalFormat df1 = new DecimalFormat( "#.######" );
	private static int num_records = 100;
	private final static int num_attributes = 1;
	private final static int max_val = 10000;
	
	private final static int total = 10000;
	private final static String ATTR_PREFIX = "a";
	private final static String IDX_PREFIX = "i";
	
	private final static Random rand = new Random();
	
	private static double fraction = 0.01;
	
	private static boolean toInsert = true;
	private static boolean toClean = false;
	
	static int rcvd = 0;
	static synchronized void incr() {
		rcvd++;
	}
	static CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<Integer>();
	static CopyOnWriteArrayList<Double> latency = new CopyOnWriteArrayList<Double>();
	
	private static Connection connect;
	private final static String passwd = "dbuserpassword";
	
	private static void initDB(String host, String dbname) {
		try {
			//
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager
					.getConnection("jdbc:mysql://"+host+"/"+dbname, "umass", passwd);	
			// createTable("0");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static boolean createTable(String table_name) throws SQLException {
		Statement stmt = connect.createStatement();
		String cmd = "CREATE TABLE IF NOT EXISTS "+table_name+" (";
		for (int i=0; i<num_attributes; i++) {
			if(i==0)
				cmd = cmd +ATTR_PREFIX+i+" DOUBLE";
			else
				cmd = cmd + "," +ATTR_PREFIX+i+" DOUBLE";
		}
		cmd = cmd + ");";
		// in memory database
		// cmd = cmd + ") ENGINE=MEMORY;";
		stmt.executeUpdate(cmd);
		return true;
	}
	
	private static void createIndex(String table_name, int index) throws SQLException {
		// "CREATE INDEX index_name ON table_name (column1, column2, ...);";
		
		String index_name = IDX_PREFIX + index;
		Statement stmt = connect.createStatement();
		String cmd = "CREATE INDEX "+index_name+" ON "+table_name+" (";
		cmd = cmd + ATTR_PREFIX+index;
		cmd = cmd + ");";
		
		boolean success = stmt.execute(cmd);
		if(success)
			System.out.println("Index "+index+" is created successfully!");
	}
	
	private static void dropTable(String table_name) throws SQLException {
		// "DROP INDEX table_name.index_name;";
		
		Statement stmt = connect.createStatement();
		String cmd = "DROP TABLE "+table_name+";";
		
		boolean success = stmt.execute(cmd);
		if(success)
			System.out.println("Table "+table_name+" is dropped successfully!");		
	}
	
	private static class QueryRunnable implements Runnable {
		final String table_name;
		Statement stmt;
		
		public QueryRunnable(Connection connection, String table_name) {
			this.table_name = table_name;
			try {
				this.stmt = connection.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			// executor.execute(new QueryRunnable(connect, table_name));
			String attr = ATTR_PREFIX+rand.nextInt(num_attributes);
			String cmd = "SELECT * FROM "+table_name+" WHERE "+attr+" BETWEEN ";
			double drift = max_val*fraction;
			double start = rand.nextDouble()+rand.nextInt(max_val);
			double end = start + drift;
			cmd = cmd + start + " AND "+end+";";
			// System.out.println("QUERY:"+cmd);
			
			int cnt = 0;
			List<Integer> result_set = new ArrayList<Integer>();
			long t = System.nanoTime();
			try {
				ResultSet result = stmt.executeQuery(cmd);
				while (result.next()) {
					result_set.add( result.getInt(attr) );
					cnt ++; 
			    }
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long elapsed = System.nanoTime()-t;
			// System.out.println("Total:"+cnt+",elapsed:"+(System.nanoTime()-t)/1000.0+"us");
			incr();
			list.add(cnt);
			latency.add(elapsed/1000.0);
			try {
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws InterruptedException, SQLException {
		if(args.length == 0){
			System.out.println("Please provide the host name.");
			System.exit(0);
		}
		
		if (System.getProperty("num_records") != null) {
			num_records = Integer.parseInt(System.getProperty("num_records"));
		}		

		if (System.getProperty("insert") != null) {
			toInsert = Boolean.parseBoolean(System.getProperty("insert"));
		}
		
		if (System.getProperty("clean") != null) {
			toClean = Boolean.parseBoolean(System.getProperty("clean"));
		}
		
		if (System.getProperty("frac") != null) {
			fraction = Double.parseDouble(System.getProperty("frac"));
		}
		
		String host = args[0];
		
		initDB(host, "active");
		
		final String table_name = "test_"+num_records/10000;
		
		ExecutorService executor = Executors.newFixedThreadPool(100);
		
		if(toInsert) {
			double interval = ((double) max_val )/num_records;
			
			// create table
			createTable(table_name);
			
			// insert records
			Statement stmt = connect.createStatement();
			for(int i=0; i<num_records; i++) {
				String cmd = "INSERT INTO "+table_name +" (";
				for (int k=0; k<num_attributes; k++) {
					if(k==0)
						cmd = cmd + ATTR_PREFIX + k; 
					else
						cmd = cmd +"," + ATTR_PREFIX + k;
					
				}
				cmd = cmd + ") VALUES(";
				for (int k=0; k<num_attributes; k++){
					if(k==0)
						cmd = cmd + df1.format(i*interval); //rand.nextInt(max_val);
					else
						cmd = cmd + "," + rand.nextInt(max_val);
				}
				cmd = cmd + ");";				
				
				stmt.execute(cmd);
			}
			System.out.println("All "+num_records+" have been inserted successfully!");
			
			// create index 
			for (int k=0; k<num_attributes; k++){
				createIndex(table_name, k);
			}
		}
		// System.exit(0);
		
		long begin = System.currentTimeMillis();
		int count = 0; 
		while(count < total) {
			Statement stmt = connect.createStatement();			
			// executor.execute(new QueryRunnable(connect, table_name));
			String attr = ATTR_PREFIX+rand.nextInt(num_attributes);
			String cmd = "SELECT * FROM "+table_name+" WHERE "+attr+" BETWEEN ";
			double drift = max_val*fraction;
			double start = rand.nextDouble()+rand.nextInt(max_val);
			double end = start + drift;
			cmd = cmd + start + " AND "+end+";";
			// System.out.println("QUERY:"+cmd);
			
			int cnt = 0;
			List<Integer> result_set = new ArrayList<Integer>();
			long t = System.nanoTime();
			try {
				ResultSet result = stmt.executeQuery(cmd);
				while (result.next()) {
					result_set.add( result.getInt(attr) );
					cnt ++; 
			    }
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long elapsed = System.nanoTime()-t;
			// System.out.println("Total:"+cnt+",elapsed:"+(System.nanoTime()-t)/1000.0+"us");
			incr();
			list.add(cnt);
			latency.add(elapsed/1000.0);
			try {
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			count++;
		}
		
		while(rcvd < total) {
			System.out.println("Received:"+rcvd);
			Thread.sleep(1000);
		}
		
		double elapsed = (System.currentTimeMillis()-begin)/1000.0;
		System.out.println("Elapsed: "+elapsed);
		System.out.println("Thruput: "+rcvd/elapsed);
		
		try {
		    // System.out.println("attempt to shutdown executor");
		    executor.shutdown();
		    executor.awaitTermination(5, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
		    System.err.println("tasks interrupted");
		}
		finally {
		    if (!executor.isTerminated()) {
		        System.err.println("cancel non-finished tasks");
		    }
		    executor.shutdownNow();
		    // System.out.println("shutdown finished");
		}
		
		
		double l = 0;
		for (double lat : latency){
			l += lat;
		}
		l = l/total;
		double d=0;
		for (double lat: latency) {
			d = d+ (lat-l)*(lat-l);
		}
		System.out.println("The average latency is:"+df1.format(l)+"us, std:"+df1.format(Math.sqrt(d)/total)+"us");
		
		if(toClean)
			dropTable(table_name);

	}
}
