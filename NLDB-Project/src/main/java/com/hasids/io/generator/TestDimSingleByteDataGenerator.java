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
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.io.old.SingleByteDimDataBatchWriter;

public class TestDimSingleByteDataGenerator {
	
	private String _datasetName;
	private int _recordCount = 0;
	private int _distributions = 0;
	
	public TestDimSingleByteDataGenerator(String datasetName, int recordCount, int distributions) throws Exception{
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Record count must be > 0 and <= 2 billion");
		if (distributions > 254 || distributions <= 1)
			throw new Exception("Record count must be > 0 and < 254");
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception("Invalid file name");
		this._datasetName = datasetName;
		this._recordCount = recordCount;
		this._distributions = distributions;
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
	public void generateData() throws Exception {
		
		// track the beginning time of the job
		long beginTime = System.nanoTime();
		// track the end time of the job
		long endTime = System.nanoTime();
						
		try {
			
			// writing in batch mode
			SingleByteDimDataBatchWriter writer = new SingleByteDimDataBatchWriter(this._datasetName, this._recordCount);
			byte z;
			int[] positions = new int[this._recordCount];
			byte[] values = new byte[this._recordCount];
			for (int i = 0; i < this._recordCount; i++) {
				// distributions = number of unique characters generated for distribution
				// null is reserved for no data
				// generate a random number which is between 0.0000000001 - 0.9999999999
				// To get a random distribution upto 255, the random will have to be
				// multiplied by 1000 and the result is the remainder when divided by distributions
				// or number of domain vales.
				z = (byte)(((Math.random() * 1000) % this._distributions) + 1);	
				
				// put the data into an array
				positions[i] = i;
				values[i] = z;
				
			}
			
			writer.setWriteDataPositionBuffer(positions, values, false, false);
			writer.writeToSegment();
			
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
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Tester, 3 distributions (domain of 3 values)
		/*String filename = "c:\\users\\dpras\\tempdata\\tester";
		try {
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 100, 3);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}*/
		
		
		// Days, 31 days, 1-31 (domain of 31 values)
		String filename = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
		try {
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 31);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// 12 months, 1- Jan....12 - Dec (domain of 12 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
		try {
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 12);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// 10 years distributed (domain of 10 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
		try {
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 10);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
		// customer type; 1 - Male, 2 - Female (domain of 2 values)
		filename = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
		try {
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 2);
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
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 5);
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
			TestDimSingleByteDataGenerator tdg = 
					new TestDimSingleByteDataGenerator(filename, 10000000, 6);
			tdg.generateData();
		}
		catch (Exception e) {
			System.err.println (e.getMessage());
			e.printStackTrace();
		}
		
	}
}
