/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.generator;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.dim.DimDataWriter;

public class FactDataGenerator {
	
	private String _dbName;
	private String _datasetName;
	private int _recordCount = 0;
	private int _distributions = 0;
	private int _encoding = 0;
	private int _segmentNo;
	private int _segmentCount = 0;
	private boolean _createSegment = false;
	private int _startRecordId = 0;
	
	public FactDataGenerator(String dbName, String datasetName, int segmentCount, int recordCount, int distributions, int encoding, int segmentNo, boolean createSegment, int startRecordId) throws Exception{
		int dataLength = 1;
		if (encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)
			dataLength = 2;
		else if (encoding == CheckSum.FACT_ENCODE_TYPE_INT || encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT)
			dataLength = 4;
		else if (encoding == CheckSum.FACT_ENCODE_TYPE_LONG || encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			dataLength = 8;
		else if (encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
			int length = 0;
			if (length <= 0 || length > HASIDSConstants.FACT_MAX_ALPHAN_LENGTH)
				throw new Exception("Alpha type cannot have a length <= 0 or > " + HASIDSConstants.FACT_MAX_ALPHAN_LENGTH);
			dataLength = length;
		}
		
		int maxRecords = HASIDSConstants.DIM_MAX_RECORDS/dataLength;
		
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Record count must be > 0 and <= 2 billion");
		if (segmentCount <= 0 || segmentCount > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Segment count must be > 0 and <= 2 billion");
		if (startRecordId < 0 || startRecordId > HASIDSConstants.DIM_MAX_RECORDS-1)
			throw new Exception("Start record Id must be >= 0 and < 2 billion");
		if (distributions > 2000000 || distributions <= 1)
			throw new Exception("Distributions must be > 1 and < " + Short.MAX_VALUE);
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception("Invalid file name");
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception("Invalid database name");
		if (encoding != CheckSum.DIM_ENCODE_TYPE1 && encoding != CheckSum.DIM_ENCODE_TYPE2 && encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception("Invalid Encoding");
		if (segmentNo < 0)
			throw new Exception("Segment number cannot be < 0");
		
		this._dbName = dbName;
		this._datasetName = datasetName;
		this._recordCount = recordCount;
		this._distributions = distributions;
		this._encoding = encoding;
		this._segmentNo = segmentNo;
		this._segmentCount = segmentCount;
		this._createSegment = createSegment;
		this._startRecordId = startRecordId;
	}
	
	/**
	 * 
	 * @return String representing the dataset name
	 */
	public String getDatasetName() {
		return this._datasetName;
	}
	
	/**
	 * 
	 * @return int representing the record count associated with the dataset
	 */
	public int getRecordCount() {
		return this._recordCount;
	}
	
	/**
	 * 
	 * @return int representing the number of domain values for a dimension
	 */
	public int getDistributions() {
		return this._distributions;
	}
	
	/**
	 * The method generates data using the BufferedOutputStream method. This method of 
	 * data writing is not effecient when compared to MemoryMapping method via file channels.
	 * A random number is generated to represent a domain value and is assigned as ASCII
	 * code ranging from 1 to number of distributions, which for single byte ASCII set should 
	 * be 255. For Domain values more than 255, the file should be encoded using UTF-16 which
	 * allows up to (2 power 15) -1 values = 32767 values. In UTF-16, two bytes are used to
	 * store a characte.
	 * 
	 * @throws Exception
	 */
	public Hashtable<Integer, int[]> generateData() throws Exception {
		
		Hashtable<Integer, int[]> h = new Hashtable<Integer, int[]>();
		// track the beginning time of the job
		long beginTime = System.nanoTime();
		// track the end time of the job
		long endTime = System.nanoTime();
		
		DimDataWriter writer = null;
						
		try {
			
			// writing in batch mode
			if (this._createSegment)
				writer = new DimDataWriter(this._dbName, this._datasetName, this._segmentCount, this._encoding, 0, (short)0, this._segmentNo);
			else
				writer = new DimDataWriter(this._dbName, this._datasetName, HASIDSConstants.OPERATION_MODE_BATCH);
			
			endTime = System.nanoTime();
			
			int[] positions = new int[this._recordCount];
			int[] values = new int[this._recordCount];
			int currentRecordId = this._startRecordId - 1;
			
			if (this._encoding == CheckSum.DIM_ENCODE_TYPE1) {
				byte z = 0;
				for (int i = 0; i < this._recordCount; i++) {
					// distributions = number of unique characters generated for distribution
					// null is reserved for no data
					// generate a random number which is between 0.0000000001 - 0.9999999999
					// To get a random distribution upto 255, the random will have to be
					// multiplied by 1000 and the result is the remainder when divided by distributions
					// or number of domain vales.
					z = (byte)(((Math.random() * 1000000) % this._distributions) + 1);	
					
					// put the data into an array
					++currentRecordId;
					positions[i] = currentRecordId;
					values[i] = z;
					
				}
			}
			else if (this._encoding == CheckSum.DIM_ENCODE_TYPE2) {
				short z = 0;
				for (int i = 0; i < this._recordCount; i++) {
					// distributions = number of unique characters generated for distribution
					// null is reserved for no data
					// generate a random number which is between 0.0000000001 - 0.9999999999
					// To get a random distribution upto 255, the random will have to be
					// multiplied by 1000 and the result is the remainder when divided by distributions
					// or number of domain vales.
					z = (short)(((Math.random() * 1000000) % this._distributions) + 1);	
					
					// put the data into an array
					++currentRecordId;
					positions[i] = currentRecordId;
					values[i] = z;
					
				}
			}
			else if (this._encoding == CheckSum.DIM_ENCODE_TYPE3) {
				int z = 0;
				for (int i = 0; i < this._recordCount; i++) {
					// distributions = number of unique characters generated for distribution
					// null is reserved for no data
					// generate a random number which is between 0.0000000001 - 0.9999999999
					// To get a random distribution upto 255, the random will have to be
					// multiplied by 1000 and the result is the remainder when divided by distributions
					// or number of domain vales.
					z = (int)(((Math.random() * 1000000) % this._distributions) + 1);	
					
					// put the data into an array
					++currentRecordId;
					positions[i] = currentRecordId;
					values[i] = z;
					
				}
			}
			
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((System.nanoTime() - endTime), TimeUnit.NANOSECONDS);
			System.out.println("Random data generation time for : " + this._datasetName + "(" + this._recordCount + ") = " + elapsedTimeInMillis + "  Milliseconds");
			
			writer.setWriteDataPositionBuffer(positions, values, false, false);
			writer.writeToSegment(true);
			
			h = writer.getCollectionTable();
			
			writer = null;
			endTime = System.nanoTime();
		}
		catch (IOException e) {
			throw new Exception("Error writing to file : " + this._datasetName + e.getMessage());
		}
		finally {
			File f = new File(this._datasetName);
			System.out.println("File length after write : " + f.length());
			
			endTime = System.nanoTime();
			
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("File creation time for : " + this._datasetName + "(" + this._recordCount + ") = " + elapsedTimeInMillis + "  Milliseconds");
		}
		
		return h;
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		String dbName = "Test";
		int encoding = CheckSum.DIM_ENCODE_TYPE1;
		int recordCount = 1000;
		int segmentCount = 1000;
		int segmentNo = 0;
		
		
		// Days, 31 days, 1-31 (domain of 31 values)
		String filename = "c:\\users\\dpras\\tempdata\\dom_1.DM";
		try {
			FactDataGenerator tdg = 
					new FactDataGenerator(dbName, filename, segmentCount, recordCount, 31, encoding, segmentNo, true, 0);
			Hashtable<Integer, int[]> h = tdg.generateData();
			
			int key = -1;
			String keyString = null;
			int[] values = null;
			Enumeration<Integer> e = h.keys();
			while(e.hasMoreElements()) {
				key = e.nextElement();
				keyString = Integer.toString(key);
				values = h.get(key);
				System.out.println("Key : " + keyString + ", low = " + values[0] + ", high = " + values[1] + ", count = " + values[2]);
			}
			
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// 12 months, 1- Jan....12 - Dec (domain of 12 values)
		/*filename = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 12, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// 5 years distributed (domain of 5 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 10, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// customer type; 1 - Male, 2 - Female (domain of 2 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 2, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// Race; 1 - White, 2 - Asian, 3 - Black, 4 - Hispanic, 5 - Pac. Islandar
		// (domain of 5 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\race_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 5, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// customer type; 1 - Normal, 2 - Bronze, 3 - Silver, 4 - Gold, 5 - Platinum, 6 - Platinum Plus
		//(domain of 6 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 6, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// State; 50 states
		filename = "c:\\users\\dpras\\tempdata\\testdata\\state_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 50, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}		
		
		// 43000 zip codes
		encoding = CheckSum.DIM_ENCODE_TYPE2;
		filename = "c:\\users\\dpras\\tempdata\\testdata\\zip_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 200, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// 3142 counties
		encoding = CheckSum.DIM_ENCODE_TYPE3;
		filename = "c:\\users\\dpras\\tempdata\\testdata\\county_1.DM";
		try {
			DimDataGenerator tdg = 
					new DimDataGenerator(dbName, filename, recordCount, 400, encoding, segmentNo);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}*/		
	}
}
