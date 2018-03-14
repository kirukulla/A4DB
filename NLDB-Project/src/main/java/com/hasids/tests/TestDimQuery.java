package com.hasids.tests;

import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.io.dim.DimReaderWrapper;

public class TestDimQuery {

	public TestDimQuery() {
		// TODO Auto-generated constructor stub
	}

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
			
		}
		catch (Exception e) {
			System.out.println("Error : " + e.getMessage());
			e.printStackTrace();
		}
	}

}
