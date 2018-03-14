/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids.io.uni;

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
/**
 * @author dpras
 *
 */

public class UniReaderWrapper  {

	private String _dbName;
	private String _queryName;
	private String[] _filenames;
	private int _lowRange;
	private int _highRange;
	
	private int _noParallelFileReadThreads = 4;
	private boolean _parallelFileReads = true;
	
	Hashtable<String, BitSet> _h;
	Hashtable<String, ArrayList<String>> _h1;
	
	
	public UniReaderWrapper(String dbName, String queryName, String[] filenames, int lowRange, int highRange) throws Exception {
		super();
		
		if (dbName == null || dbName.trim().length() == 0)
			throw new Exception ("Database name cannot be null or empty");
		if (queryName == null || queryName.trim().length() == 0)
			throw new Exception ("Query name cannot be null or empty");
		if (filenames == null)
			throw new Exception("Filename array is null");
		if (filenames.length <= 0)
			throw new Exception("Filename array length must be >= 1");
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
	
	public Hashtable<String, BitSet> getResultBitSets() {
		return this._h;
	}
	
	public Hashtable<String, ArrayList<String>> getResultSets() {
		return this._h1;
	}
	
	public void readData() {
		
		_h = new Hashtable<String, BitSet>(this._filenames.length);
		_h1 = new Hashtable<String, ArrayList<String>>(this._filenames.length);
		int maxLength = 0;
		
		// from the low range and high range compute the bitset size
		int bitsetSize = HASIDSConstants.UNI_MAX_BITSET_SIZE;
		
		// for unary datasets, length is a variable
		// therefore the ranges must be set per file on an individual basis 
		// and not on the bitset size or maximum size
		
		// Generate new BitSets and ArrayLists for each file
		for(int i = 0; i < this._filenames.length; i++) {
			_h.put(this._filenames[i], new BitSet(bitsetSize));
			_h1.put(this._filenames[i], new ArrayList<String>());
		}
		
		try {
			
			// Create a thread group
			ThreadGroup tg = new ThreadGroup(this._queryName);
		
			
			// Read each file in parallel based on number of parallel threads
			for (int i = 0; i < this._filenames.length; i++) {
				
				int noParallelReads = this._noParallelFileReadThreads;
				// determine the file length
				String filename = this._filenames[i];
				File f = new File(filename);
				int fileLength = (int) f.length();
				
				// determine if this is document file or a OLAP file
				int[] fileType = new int[1];
				int[] encoding = new int[1];
				int[] datasize = new int[1];
				short[] decimals = new short[1];
				int[] segmentCount = new int[1];
				int[] segmentNo = new int[1];
				
				CheckSum.validateUNIFile(this._dbName, filename, fileType, encoding, datasize, decimals, segmentNo, segmentCount);
				
				// get the record id length
				int recordIdLength = datasize[0] + decimals[0];
				
				// get the current record count
				int currentRecordCount = (fileLength - CheckSum.UNIFILE_CHECKSUM_LENGTH)/recordIdLength;
				
				// how many ranges will there be using the minimum range record count
				int maxRanges = (currentRecordCount / HASIDSConstants.UNI_MIN_PARALLEL_RECORD_COUNT);
				if (maxRanges == 0) maxRanges = 1;
				
				// check if we have optimal ranges
				if (this._noParallelFileReadThreads > maxRanges )	 
					noParallelReads = maxRanges;
				else
					noParallelReads = this._noParallelFileReadThreads;
				
				int readSize = currentRecordCount/noParallelReads;
				int start = 1;
				int end = readSize;
				if (currentRecordCount%noParallelReads != 0)
					++noParallelReads;
				
				for (int j = 0; j < noParallelReads; j++) {
					
					UniDataReader tddr = new UniDataReader(this._dbName, this._filenames[i], this._lowRange, this._highRange, start, end);
					tddr.setResultSets(_h.get(this._filenames[i]), _h1.get(this._filenames[i]));
					Thread t = new Thread(tg, tddr);
					t.setPriority(Thread.MAX_PRIORITY);
					t.start();
					
					start = start + readSize;
					end = end + readSize;
					
					if (end > currentRecordCount)
						end = currentRecordCount;
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
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int lowRange = 1;
		int highRange = 500000000;
		int noParallelThreads = 49;

		String queryName = "OLAP Query";
		
		String dbName = "Test";
		String f1 = "c:\\users\\dpras\\tempdata\\testdata\\samsung_1.UN";
		
		String[] filenames = {f1};
		
				
		try {
			
			long startTime = System.nanoTime();
			
			UniReaderWrapper urw = new UniReaderWrapper(dbName, queryName, filenames, lowRange, highRange);
			urw.setParallelReadThreads(noParallelThreads);
			urw.setParallelFileReads(true);
			urw.readData();
			
			Hashtable<String, BitSet> h = urw.getResultBitSets();
			Hashtable<String, ArrayList<String>> h1 = urw.getResultSets();
			
			Enumeration<String> e = h.keys();
			String filename;
			while(e.hasMoreElements()) {
				filename = e.nextElement();
				System.out.println(filename + " cardinality = " + h.get(filename).cardinality());
			}
				
			long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total execution time : " + elapsedTimeInMillis);		
		}
		catch(Exception e) {
			e.printStackTrace();
		}		
	}


}
