package test;

import java.util.Random;

/**
 * @author gaozy
 *
 */
public class WorkloadTest {

	final static private int max_val = 10000;
	final static private double fraction = 0.001;
	private static Random rand = new Random();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int num_selected_attr = 10;
		
		int drift = (int) (Math.round(max_val*Math.pow(fraction, 1.0/num_selected_attr)));
		
		System.out.println(drift);
		
		for (int i=0; i<1000; i++){
			int start = rand.nextInt(max_val);
			int end = start + drift;
			// System.out.println("Range:["+start+","+end+"]");
		}
	}
	
}
