/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids.io.dim;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
/**
 * @author dpras
 *
 */

public class DimReaderWrapper  {

	private String _dbName;
	private String _queryName;
	private String[] _filenames;
	private int _lowRange;
	private int _highRange;
	private ArrayList<int[]> _filter;
	private int _ranges[][];
	
	private int _noParallelFileReadThreads = 4;
	private int _noSegmentParallelThreads = 4;
	
	private boolean _parallelFileReads = true;
	
	public DimReaderWrapper(String dbName, String queryName, String[] filenames, ArrayList<int[]> filter, int lowRange, int highRange) throws Exception {
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
		if (filter.size() <= 0)
			throw new Exception("Filter array length must be >= 1");
		if (filenames.length != filter.size())
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
			
			//int[] fileFilter = filter.get(i);
			//if (fileFilter == null || fileFilter.length <= 0)
			//	throw new Exception ("There are no filters for " + f.getName());
		}
		
		this._dbName = dbName;
		this._queryName = queryName;
		this._filenames = filenames;
		this._lowRange = lowRange;
		this._highRange = highRange;
		this._filter = filter;
	}
	
	public void setParallelReadThreads(int noParallelFileReadThreads, int noSegmentParallelThreads) throws Exception {
		
		if (noParallelFileReadThreads > HASIDSConstants.MAX_PARALLELFILEREAD_THREADS)
			throw new Exception ("Number of maximum parallel read thread distribution per file cannot exceed " + HASIDSConstants.MAX_PARALLELFILEREAD_THREADS);
	
		if (noSegmentParallelThreads > HASIDSConstants.MAX_PARALLELSEGMENTREAD_THREADS)
			throw new Exception ("Number of parallel threads per file at a time cannot exceed " + HASIDSConstants.MAX_PARALLELSEGMENTREAD_THREADS);
	
		if (noParallelFileReadThreads < 1)
			throw new Exception ("Number of parallel read threads per file cannot be less than one!");
		
		if (noSegmentParallelThreads < 1)
			throw new Exception ("Number of parallel read threads per segment cannot be less than one!");
		
		this._noParallelFileReadThreads = noParallelFileReadThreads;
		this._noSegmentParallelThreads = noSegmentParallelThreads;
	}
	
	public void setParallelFileReads(boolean parallelFileReads) {
		this._parallelFileReads = parallelFileReads;
	}
	
	/**
	 * Method to get the intersection of the filenames used in the constructor
	 * @return
	 */
	public int[] getIntersection() {
		int[] retVal = null;
		
		Hashtable<String, BitSet> h = this.getBOBSSResultSet();
		
		BitSet b = null;
		String filename = null;
		Enumeration<String> e = h.keys();
		
		if (h != null) {
			while (e.hasMoreElements()) {
				filename = e.nextElement();
				if (b == null)
					b = h.get(filename);
				else
					b.and(h.get(filename));
			}
		}
		
		retVal = b.stream().toArray();
		return retVal;
	}
	
	/**
	 * Method to get the BOSS Resultsets associated with each file input in the constructor
	 * @return
	 */
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
		//System.out.println("Number of ranges set : " + ranges.length);
		this._ranges = ranges;
		
		for(int i = 0; i < this._filenames.length; i++)
			h.put(this._filenames[i], new BitSet(bitsetSize));
		
		//System.out.println("Number of files in hashtable for processing : " + h.size());
		//for (int i = 0; i < ranges.length; i++)
		//	System.out.println(" i/lowRange/HighRange = " + i + " / " + ranges[i][0] + " / " + ranges[i][1]);
		
		
		try {
			
			// Create a thread group
			ThreadGroup tg = new ThreadGroup(this._queryName);
			
			//System.out.println("Number of files being processed : " + this._filenames.length);
		
			// Read each file in parallel based on number of parallel threads
			for (int i = 0; i < this._filenames.length; i++) {
				
				System.out.println("Processing file : " + this._filenames[i]);
				DimReaderWrapperMPP mpp = new DimReaderWrapperMPP(this._dbName, this._filenames[i], this._filter.get(i), this._ranges, h.get(this._filenames[i]), this._noSegmentParallelThreads);
				
				Thread t = new Thread(tg, mpp);
				t.setPriority(Thread.NORM_PRIORITY);
				t.start();
				//t.join();
				
				// if parallel files is set to false
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
			e.printStackTrace();
		}
		
		return h;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int lowRange = 1;
		int highRange = 500000000;
		int segmentThreadSize = 10000000;
		
		
		int noFileParallelThreads = (highRange - lowRange + 1)/segmentThreadSize; //  no of threads segments to run at a time at a time
		if (noFileParallelThreads <= 0)
			noFileParallelThreads = 1;
		
		int noSegmentParallelThreads = noFileParallelThreads * 4; // maximum number of threads to start
		if (noSegmentParallelThreads <= 0)
			noSegmentParallelThreads = 1;
		
		String queryName = "Customer Query";
		
		
		int[] c = {1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31};
		int[] c1 = {2,4,6,8,10,12};
		int[] c2 = {1,3,5,7,9};
		int[] c3 = {2};
		int[] c4 = {1,2,3,5};
		int[] c5 = {2,3};
		int[] c6 = {1,2,3,4,5,6,12,13,14,15,16,17,18,24,25,26,27,28,29,30,36,37,38,39,40,41,42}; // state 50
		int[] c7 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100}; // County No 200
		int[] c8 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,150,175,200,225,245}; // zip no 400
		
		/*
		int[] c = {1};
		int[] c1 = {2};
		int[] c2 = {1};
		int[] c3 = {2};
		int[] c4 = {1};
		int[] c5 = {2};
		int[] c6 = {1}; // state 50
		int[] c7 = {1}; // County No 200
		int[] c8 = {1}; // zip no 400
		*/
		
		
		ArrayList<int[]> filter = new ArrayList<int[]>();
		filter.add(c);
		filter.add(c1);
		filter.add(c2);
		filter.add(c3);
		filter.add(c4);
		filter.add(c5);
		filter.add(c6);
		filter.add(c7);
		filter.add(c8);
		
		/*filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);
		filter.add(null);*/
		
		
		
		String dbName = "Test";
		String f4 = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
		String f2 = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
		String f3 = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
		String f1 = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
		String f5 = "c:\\users\\dpras\\tempdata\\testdata\\race_1.DM";
		String f6 = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
		String f7 = "c:\\users\\dpras\\tempdata\\testdata\\state_1.DM";
		String f8 = "c:\\users\\dpras\\tempdata\\testdata\\county_1.DM";
		String f9 = "c:\\users\\dpras\\tempdata\\testdata\\zip_1.DM";
		//String[] filenames = {f1, f2, f3, f4, f5, f6, f7, f8, f9};
		String[] filenames = {f1, f2, f3, f4, f5, f6, f7, f8, f9};
		try {
			BitSet resultSet = null;
			
			long beginTime = System.nanoTime();
			
			DimReaderWrapper tqe = new DimReaderWrapper(dbName, queryName, filenames, filter, lowRange, highRange);
			tqe.setParallelReadThreads(noFileParallelThreads, noSegmentParallelThreads);
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
					else {
						resultSet.and(b);
						System.out.println("Intersection results with previous set = " + resultSet.cardinality());
					}
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

class DimReaderWrapperMPP implements Runnable {
	
	private int _status = HASIDSConstants.THREAD_INACTIVE;
	private String _datasetName;
	private String _dbName;
	
	private int _encoding;
	private int _segmentNo;
	private int _dataLength;
	private BitSet _computedBitSet;
	private ThreadGroup _tg;
	
	private String _classDescription;
	private int _noSegmentParallelThreads = 1;
	
	int _ranges[][];
	
	private int[] _filter;
	
	public DimReaderWrapperMPP(String dbName, String datasetName, int[] filter, int ranges[][], BitSet b, int noSegmentParallelThreads) throws Exception {
		super();
		
		DimDataReader ddr = new DimDataReader(dbName, datasetName, 1, 1 );
		
		this._dbName = dbName;
		this._datasetName = datasetName;
		this._filter = filter;
		this._dataLength = ddr.getDataLength();
		this._segmentNo = ddr.getSegmentNo();
		this._encoding = ddr.getEncoding();
		this._computedBitSet = b;
		this._noSegmentParallelThreads = noSegmentParallelThreads;
		//System.out.println("Size of received BitSet : " + this._computedBitSet.size());
		
		this._ranges = ranges;
		//this._tg = tg;
		
		if (ranges.length <= 0)
			throw new Exception("Invalid ranges specified!");
		
		ddr = null;
		this._classDescription = this.getClass().getName() + "//Database Name : " + this._dbName + ", Dataset Name : " + this._datasetName + 
				", Ranges : " + this._ranges.length;
		
		//System.out.println("In constructor of " + this._classDescription);
	}
	
	public int getStatus() {
		return this._status;
	}
	
	public String getClassDescription () {
		return this._classDescription;
	}
	
	public ThreadGroup getThreadGroup() {
		return this._tg;
	}

	public void run() {
		// TODO Auto-generated method stub
		
		//System.out.println("Thread started : " + this._classDescription);
		this._tg = new ThreadGroup(this._classDescription);
		
		try {
			for (int j = 0; j < this._ranges.length; j++) {
				DimDataReader tddr = new DimDataReader(this._dbName, this._datasetName, this._ranges[j][0], this._ranges[j][1]);
				tddr.setComputedBitSet(this._computedBitSet);
				tddr.setFilter(this._filter);
				
				//tddr.setGTFilter(0);
				Thread t = new Thread(this._tg, tddr);
				t.setPriority(Thread.MIN_PRIORITY);
				t.start();
				//t.join();
				//System.out.println("Thread );
				
				if (this._noSegmentParallelThreads == 1 || (j+1)%this._noSegmentParallelThreads == 0) {
					//System.out.println("Sleep induced at thread count : " + (j + 1) + " for " + this._classDescription);
					while(this._tg.activeCount() > 0) {
						//System.out.println("No of active threads : " + this._tg.activeCount());
						Thread.sleep(10); // sleep for 10 millis
					}
				}
			}
		}
		catch (Exception e) {
			this._status = HASIDSConstants.THREAD_FAILED;
			e.printStackTrace();
			return;
		}
		
		this._status = HASIDSConstants.THREAD_COMPLETE;
	}
	
}
