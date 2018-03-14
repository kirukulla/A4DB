/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;

public class TestUNIDataGenerator {
	
	private String _datasetName;
	private long _startId = 0;
	private long _endId = 0;
	private int _spacing = 0;
	private int _recordCount = 0;
	
	public TestUNIDataGenerator(String datasetName, long startId, long endId, int spacing) throws Exception {
		// TODO Auto-generated constructor stub
		if ((endId - startId + 1) > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Record count must be > 0 and <= " + HASIDSConstants.DIM_MAX_RECORDS);
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception("Invalid file name");
		if (startId <= 0 || endId <= 0)
			throw new Exception("Start and end Ids must be > 0");
		if (startId > endId)
			throw new Exception("Start Id cannot be > end Id");
		if (spacing <= 0 )
			throw new Exception("Spacing between two identifiers must be > 0");
		this._datasetName = datasetName;
		this._startId = startId;
		this._endId = endId;
		this._spacing = spacing;
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
	 * @return long representing the start Identifier associated with the dataset
	 */
	public long getStartId() {
		return this._startId;
	}
	
	/**
	 * 
	 * @return long representing the end Identifier associated with the dataset
	 */
	public long getEndId() {
		return this._endId;
	}
	
	
	/**
	 * 
	 * @return int representing the spacing between identifiers in the dataset
	 */
	public int getSpacing() {
		return this._spacing;
	}
	
	public void generateData() throws Exception {
		
		// track the beginning time of the job
		long beginTime = System.nanoTime();
		// track the end time of the job
		long endTime = System.nanoTime();
		
		
		BufferedWriter b = null;
		try {
			b = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this._datasetName)));

			int z;
			for (long i = this._startId; i < this._endId; i = i + this._spacing) {
				b.write(i + System.lineSeparator());
				++this._recordCount;
			}
			
			b.flush();
			b.close();
			endTime = System.nanoTime();
		}
		catch (IOException e) {
			throw new Exception("Error writing to file : " + this._datasetName + e.getMessage());
		}
		finally {
			if (b != null) {
				try {
					b.close();
					File f = new File(this._datasetName);
					System.out.println("File length after write : " + f.length());
				}
				catch(IOException e) {
					throw new Exception("Error writing to file : " + this._datasetName + e.getMessage());
				}
			}
			
			endTime = System.nanoTime();
			
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("File creation time for : " + this._datasetName + "(" + this._recordCount + ") = " + elapsedTimeInMillis + "  Milliseconds");       
		}
	}



	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String filename = "c:\\users\\dpras\\tempdata\\unary\\opthalmology.UN";
		long startId = 2;
		long endId = 10000;
		int spacing = 400;
		try {
			TestUNIDataGenerator t = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t.generateData();
			
			startId = 2;
			spacing = 50;
			filename = "c:\\users\\dpras\\tempdata\\unary\\branch.UN";
			TestUNIDataGenerator t1 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t1.generateData();
			
			startId = 2;
			spacing = 100;
			filename = "c:\\users\\dpras\\tempdata\\unary\\medicine.UN";
			TestUNIDataGenerator t2 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t2.generateData();
			
			startId = 2;
			spacing = 40;
			filename = "c:\\users\\dpras\\tempdata\\unary\\eye.UN";
			TestUNIDataGenerator t3 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t3.generateData();
			
			startId = 2;
			spacing = 1000;
			filename = "c:\\users\\dpras\\tempdata\\unary\\anatomy.UN";
			TestUNIDataGenerator t4 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t4.generateData();
			
			startId = 2;
			spacing = 500;
			filename = "c:\\users\\dpras\\tempdata\\unary\\specialist.UN";
			TestUNIDataGenerator t5 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t5.generateData();
			
			startId = 2;
			spacing = 250;
			filename = "c:\\users\\dpras\\tempdata\\unary\\disease.UN";
			TestUNIDataGenerator t6 = new TestUNIDataGenerator(filename, startId, endId, spacing);
			t6.generateData();
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
