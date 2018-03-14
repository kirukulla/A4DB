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

public class GenerateTaxiTripsTestData {
	
	private String _dbName;
	private String _datasetName;
	private int _segmentCount = 0;
	private int _recordCount = 0;
	private int _distributions = 0;
	private int _encoding = 0;
	private int _segmentNo;
	private int _startRecordId = 0;
	private int _distributionId = 0;
	
	
	public GenerateTaxiTripsTestData(String dbName, String datasetName, int segmentCount, int recordCount, int distributions, int encoding, int segmentNo, int startRecordId, int distributionId) throws Exception{
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Record count must be > 0 and <= 2 billion");
		if ((distributions <= 1 && distributionId <= 0) || (distributionId > 0 && distributions > 1))
			throw new Exception("Distributions must be > 1 and < 256 or should be 1 when distributionId > 0");
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception("Invalid file name");
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception("Invalid database name");
		if (encoding != CheckSum.DIM_ENCODE_TYPE1 && encoding != CheckSum.DIM_ENCODE_TYPE2 && encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception("Invalid Encoding");
		if (segmentNo < 0)
			throw new Exception("Segment number cannot be < 0");
		if ((recordCount + startRecordId) >  HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Record ids cannot exceed " + HASIDSConstants.DIM_MAX_RECORDS);
		
		this._dbName = dbName;
		this._datasetName = datasetName;
		this._segmentCount = segmentCount;
		this._recordCount = recordCount;
		this._distributions = distributions;
		this._encoding = encoding;
		this._segmentNo = segmentNo;
		this._startRecordId = startRecordId;
		this._distributionId = distributionId;
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
	public Hashtable<Integer, int[]> generateData(boolean generateSegment) throws Exception {
		
		Hashtable<Integer, int[]> h = new Hashtable<Integer, int[]>();
		// track the beginning time of the job
		long beginTime = System.nanoTime();
		// track the end time of the job
		long endTime = System.nanoTime();
					
		DimDataWriter writer = null;
		try {
			
			// writing in batch mode
			if (generateSegment)
				writer = new DimDataWriter(this._dbName, this._datasetName, this._segmentCount, this._encoding, 0, (short)0, this._segmentNo);
			else
				writer = new DimDataWriter(this._dbName, this._datasetName, HASIDSConstants.OPERATION_MODE_BATCH);
			
			endTime = System.nanoTime();
			
			int[] positions = new int[this._recordCount];
			int[] values = new int[this._recordCount];
			int currentRecordId = this._startRecordId - 1;;
			
			if (this._distributionId > 0 && this._distributions == 1) {
				for (int i = 0; i < this._recordCount; i++) {
					currentRecordId++;
					positions[i] = currentRecordId;
					values[i] = this._distributionId;
				}
			}
			else {
				if (this._encoding == CheckSum.DIM_ENCODE_TYPE1) {
					byte z = 0;
					
					for (int i = 0; i < this._recordCount; i++) {
						currentRecordId++;
						// distributions = number of unique characters generated for distribution
						// null is reserved for no data
						// generate a random number which is between 0.0000000001 - 0.9999999999
						// To get a random distribution upto 255, the random will have to be
						// multiplied by 1000 and the result is the remainder when divided by distributions
						// or number of domain vales.
						
						z = (byte)(((Math.random() * 1000000) % this._distributions) + 1);	
						
						// put the data into an array
						positions[i] = currentRecordId;
						values[i] = z;
					}
				}
				else if (this._encoding == CheckSum.DIM_ENCODE_TYPE2) {
					short z = 0;
					for (int i = 0; i < this._recordCount; i++) {
						currentRecordId++;
						// distributions = number of unique characters generated for distribution
						// null is reserved for no data
						// generate a random number which is between 0.0000000001 - 0.9999999999
						// To get a random distribution upto 255, the random will have to be
						// multiplied by 1000 and the result is the remainder when divided by distributions
						// or number of domain vales.
						z = (short)(((Math.random() * 1000000) % this._distributions) + 1);	
						
						// put the data into an array
						positions[i] = currentRecordId;
						values[i] = z;
						
					}
				}
				else if (this._encoding == CheckSum.DIM_ENCODE_TYPE3) {
					int z = 0;
					for (int i = 0; i < this._recordCount; i++) {
						currentRecordId++;
						// distributions = number of unique characters generated for distribution
						// null is reserved for no data
						// generate a random number which is between 0.0000000001 - 0.9999999999
						// To get a random distribution upto 255, the random will have to be
						// multiplied by 1000 and the result is the remainder when divided by distributions
						// or number of domain vales.
						z = (int)(((Math.random() * 1000000) % this._distributions) + 1);	
						
						// put the data into an array
						positions[i] = currentRecordId;
						values[i] = z;
						
					}
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
		int segmentCount = 500000000;
		int recordCount = 500000000;
		int startRecordId = 0;
		int segmentNo = 0;
		int distributionId = 1;
		DimDataGenerator tdg = null;
		
		// years, 3 years, 1-3 (domain of 3 values)
		String filename = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_1.DM";
		/*try {
			GenerateTaxiTripsTestData tdg = 
					new GenerateTaxiTripsTestData(dbName, filename, segmentCount, recordCount, 1, encoding, segmentNo, startRecordId, distributionId);
			Hashtable<Integer, int[]> h = tdg.generateData(true);
			
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
			
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_2.DM";
			segmentNo = 1;
			startRecordId = 0;
			recordCount = 500000000;
			distributionId = 2;
			tdg = new GenerateTaxiTripsTestData(dbName, filename, segmentCount, recordCount, 1, encoding, segmentNo, startRecordId, distributionId);
			h = tdg.generateData(true);
			
			key = -1;
			keyString = null;
			values = null;
			e = h.keys();
			while(e.hasMoreElements()) {
				key = e.nextElement();
				keyString = Integer.toString(key);
				values = h.get(key);
				System.out.println("Key : " + keyString + ", low = " + values[0] + ", high = " + values[1] + ", count = " + values[2]);
			}
			
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_3.DM";
			segmentNo = 2;
			startRecordId = 0;
			recordCount = 200000000;
			distributionId = 2;
			tdg = new GenerateTaxiTripsTestData(dbName, filename, segmentCount, recordCount, 1, encoding, segmentNo, startRecordId, distributionId);
			h = tdg.generateData(true);
			
			key = -1;
			keyString = null;
			values = null;
			e = h.keys();
			while(e.hasMoreElements()) {
				key = e.nextElement();
				keyString = Integer.toString(key);
				values = h.get(key);
				System.out.println("Key : " + keyString + ", low = " + values[0] + ", high = " + values[1] + ", count = " + values[2]);
			}
			
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_3.DM";
			startRecordId = 200000000;
			recordCount = 300000000;
			distributionId = 3;
			tdg = new GenerateTaxiTripsTestData(dbName, filename, segmentCount, recordCount, 1, encoding, segmentNo, startRecordId, distributionId);
			h = tdg.generateData(false);
			
			key = -1;
			keyString = null;
			values = null;
			e = h.keys();
			while(e.hasMoreElements()) {
				key = e.nextElement();
				keyString = Integer.toString(key);
				values = h.get(key);
				System.out.println("Key : " + keyString + ", low = " + values[0] + ", high = " + values[1] + ", count = " + values[2]);
			}
			
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_4.DM";
			segmentNo = 3;
			startRecordId = 0;
			recordCount = 500000000;
			distributionId = 3;
			tdg = new GenerateTaxiTripsTestData(dbName, filename, segmentCount, recordCount, 1, encoding, segmentNo, startRecordId, distributionId);
			h = tdg.generateData(true);
			
			key = -1;
			keyString = null;
			values = null;
			e = h.keys();
			while(e.hasMoreElements()) {
				key = e.nextElement();
				keyString = Integer.toString(key);
				values = h.get(key);
				System.out.println("Key : " + keyString + ", low = " + values[0] + ", high = " + values[1] + ", count = " + values[2]);
			}
			
			h = null;
			tdg = null;
			
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		*/
		
		// 4 cab types, (domain of 4 values); 1 - yellow, 2 - green, 3 - red, 4 - uber
		/*filename = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_1.DM";
		startRecordId = 0;
		segmentNo = 0;
		encoding = CheckSum.DIM_ENCODE_TYPE1;
		segmentCount = 500000000;
		recordCount = 500000000;
		tdg = null;
		try {
			// generate the first segment
			tdg = 
					new DimDataGenerator(dbName, filename, segmentCount, recordCount, 4, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the second segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_2.DM";
			segmentNo = 1;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 4, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the third segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_3.DM";
			segmentNo = 2;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 4, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the fourth segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_4.DM";
			segmentNo = 3;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 4, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		*/

		// 6 passenger count, (domain of 6 values);
		filename = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_1.DM";
		startRecordId = 0;
		segmentNo = 0;
		encoding = CheckSum.DIM_ENCODE_TYPE1;
		segmentCount = 500000000;
		recordCount = 500000000;
		tdg = null;
		try {
			// generate the first segment
			tdg = 
					new DimDataGenerator(dbName, filename, segmentCount, recordCount, 6, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the second segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_2.DM";
			segmentNo = 1;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 6, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the third segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_3.DM";
			segmentNo = 2;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 6, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			// generate the fourth segment
			filename = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_4.DM";
			segmentNo = 3;
			tdg = new DimDataGenerator(dbName, filename, segmentCount, recordCount, 6, encoding, segmentNo, true, startRecordId);
			tdg.generateData();
			
			
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}


		// checking distributions
			
	}
}
