/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids.io.old;

import java.io.File;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import com.hasids.HASIDSConstants;
/**
 * @author dpras
 *
 */

public class SingleByteDimReaderWrapper  {

	private String _dbName;
	private String _queryName;
	private String[] _filenames;
	private int _lowRange;
	private int _highRange;
	private byte[] _filter;
	
	private int _noParallelFileReadThreads = 4;
	private boolean _parallelFileReads = true;
	
	
	public SingleByteDimReaderWrapper(String dbName, String queryName, String[] filenames, byte[] filter, int lowRange, int highRange) throws Exception {
		super();
		
		if (dbName == null || dbName.trim().length() == 0)
			throw new Exception ("Database name cannot be null or empty");
		if (queryName == null || queryName.trim().length() == 0)
			throw new Exception ("Query name cannot be null or empty");
		if (filenames == null)
			throw new Exception("Filename array is null");
		if (filter == null)
			throw new Exception("Filter array is null");
		if (filenames.length <= 0)
			throw new Exception("Filename array length must be >= 1");
		if (filter.length <= 0)
			throw new Exception("Filter array length must be >= 1");
		if (filenames.length != filter.length)
			throw new Exception ("Number of filters do not match the number of filenames");
		if (lowRange <= 0 || highRange <= 0)
			throw new Exception("Low range must be > 0 and high range must be > 0");
		if (lowRange > highRange)
			throw new Exception("Low range must be <= high range");
		if (lowRange > HASIDSConstants.DIM_MAX_RECORDS || highRange > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Low range and high range must be <= " + HASIDSConstants.DIM_MAX_RECORDS);
		
		for (int i = 0; i < filenames.length; i++) {
			File f = new File(filenames[0]);
			if (!f.exists())
				throw new Exception ("File " + filenames[i] + " does not exist!");
		}
		
		this._dbName = dbName;
		this._queryName = queryName;
		this._filenames = filenames;
		this._lowRange = lowRange;
		this._highRange = highRange;
		this._filter = filter;
	}
	
	public void setParallelReadThreads(int noParallelFileReadThreads) throws Exception{
		if (noParallelFileReadThreads > HASIDSConstants.MAX_PARALLELFILEREAD_THREADS)
			throw new Exception ("Number of parallel read threads per file cannot exceed " + HASIDSConstants.MAX_PARALLELFILEREAD_THREADS);
	
		if (noParallelFileReadThreads < 1)
			throw new Exception ("Number of parallel read threads per file cannot be less than one!");
		
		this._noParallelFileReadThreads = noParallelFileReadThreads;
	}
	
	public void setParallelFileReads(boolean parallelFileReads) {
		this._parallelFileReads = parallelFileReads;
	}
	
	public Hashtable<String, BitSet> getBOBSSResultSet() {
		Hashtable<String, BitSet> h = new Hashtable<String, BitSet>(this._filenames.length);
		
		// from the low range and high range compute the bitset size
		int bitsetSize = this._highRange - this._lowRange + 1;
		
		// determine the ranges for each parallel thread for a file;
		int rangeSize = bitsetSize/this._noParallelFileReadThreads;
		System.out.println("Bitset size : " + bitsetSize);
		System.out.println("Range size : " + rangeSize);
		
		int ranges[][] = new int[this._noParallelFileReadThreads][2];
		
		for (int i = 0; i < ranges.length; i++) {
			if (i == 0) {
				ranges[i][0] = this._lowRange;
				ranges[i][1] = ranges[i][0] - 1 + rangeSize;
			}
			else {
				if (i == (ranges.length - 1)) {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = this._highRange;
				}
				else {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = ranges[i][0] - 1 + rangeSize;
				}
			}
			
		}
		
		
		for(int i = 0; i < this._filenames.length; i++)
			h.put(this._filenames[i], new BitSet(bitsetSize));
		
		//for (int i = 0; i < ranges.length; i++)
		//	System.out.println(" i/lowRange/HighRange = " + i + " / " + ranges[i][0] + " / " + ranges[i][1]);
		
		
		try {
			
			// Create a thread group
			ThreadGroup tg = new ThreadGroup(this._queryName);
		
			// Read each file in parallel based on number of parallel threads
			for (int i = 0; i < this._filenames.length; i++) {
				for (int j = 0; j < ranges.length; j++) {
					SingleByteDimDataReader tddr = new SingleByteDimDataReader(this._dbName, this._filenames[i], ranges[j][0], ranges[j][1]);
					tddr.setComputedBitSet(h.get(this._filenames[i]));
					//tddr.setFilter(new char[this._filter[i]]);
					tddr.setFilter(new byte[] {this._filter[i]});
					Thread t = new Thread(tg, tddr);
					t.setPriority(Thread.MAX_PRIORITY);
					t.start();
					
					//if (!this._parallelFileReads)
					//	while (tg.activeCount() > 10)
					//		Thread.sleep(100);
				}
				
				// 
				if (!this._parallelFileReads)
					while (tg.activeCount() > 0)
						Thread.sleep(100);
				
				//System.out.println("Cardinality of b = " + b.cardinality());
				
			}
			
			// once all threads have started, wait till they are complete
			while (tg.activeCount() > 0)
				Thread.sleep(100);
		
		
		}
		catch (Exception e) {
			;
		}
		
		return h;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int lowRange = 1;
		int highRange = 100000001;
		int noParallelThreads = 1;
		byte[] filter = {2,7,1,5,2,2};
		String queryName = "Customer Query";
		
		String dbName = "Test";
		String f1 = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
		String f2 = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
		String f3 = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
		String f4 = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
		String f5 = "c:\\users\\dpras\\tempdata\\testdata\\race_1.DM";
		String f6 = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
		
		String[] filenames = {f1, f2, f3, f4, f5, f6};
		
		try {
			BitSet resultSet = null;
			
			long beginTime = System.nanoTime();
			
			SingleByteDimReaderWrapper tqe = new SingleByteDimReaderWrapper(dbName, queryName, filenames, filter, lowRange, highRange);
			tqe.setParallelReadThreads(noParallelThreads);
			tqe.setParallelFileReads(true);
			
			Hashtable<String, BitSet> h = tqe.getBOBSSResultSet();
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total File read time in millis : " + diff);
			
			beginTime = System.nanoTime();
			
			
			if (h != null) {
				BitSet b = null;
				String filename = null;
				Enumeration<String> e = h.keys();
				while (e.hasMoreElements()) {
					filename = e.nextElement();
					b = h.get(filename);
					System.out.println("Cardinality for " + filename + " = " + b.cardinality());
					if (resultSet == null)
						resultSet = b;
					else
						resultSet.and(b);
				}
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total Bitset intersection time in millis : " + diff);
			
			System.out.println("Cardinality of result set = " + resultSet.cardinality());
			/*int[] i = resultSet.stream().toArray();
			
			try {
				beginTime = System.nanoTime();
				TestFactDataReader t = new TestFactDataReader ("c:\\users\\dpras\\tempdata\\ordervalue",(short)16,(short)2);
				
				//System.out.println("Max Integer = " + Integer.MAX_VALUE);
				//long[] positions = {0,2,4,6,8,200,100,200000,1000000000}; //position
				t.setDataReadPositions(i);
				t.getFactData();
				double[] values = t.getValues();
				System.out.println("Number of values received from fact table= " + values.length);
				endTime = System.nanoTime();
				diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
				System.out.println("Total fact retrieval time in millis : " + diff);
				
				beginTime = System.nanoTime();
				
				double sum = 0.0;
				for (int j = 0; j < values.length; j++)
					sum += values[j];
				
				System.out.println("Sum of retrieved facts = " + sum);
				endTime = System.nanoTime();
				diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
				System.out.println("Total summation time in millis : " + diff);
					
			}
			catch(Exception e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}*/
			
		}
		catch (Exception e) {
			System.out.println("Error : " + e.getMessage());
			e.printStackTrace();
		}
	}


}
