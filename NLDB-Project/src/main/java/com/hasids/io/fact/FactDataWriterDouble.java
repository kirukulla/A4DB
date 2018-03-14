package com.hasids.io.fact;

import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataWriter;

public class FactDataWriterDouble extends DataWriter {

	public FactDataWriterDouble(String dbName, String datasetName, int operationMode) throws Exception {
		super(dbName, datasetName, operationMode);
		// TODO Auto-generated constructor stub
	}

	public FactDataWriterDouble(String dbName, String datasetName, int recordCount, int segmentNo) throws Exception {
		super(dbName, datasetName, recordCount, CheckSum.FACT_ENCODE_TYPE_DOUBLE, 8, (short)0, segmentNo);
		// TODO Auto-generated constructor stub
	}
	
	public Hashtable<Integer, Double> setWriteDataPositionBuffer(int[] position, double[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		return super.setWriteDataPositionBuffer(position, values, allowPartial, retryFlag);
	}
	
	public static Hashtable<Integer, Double> getLockedKeysDouble(String dbName, String datasetName) {
		return DataWriter.getLockedKeysDouble(dbName, datasetName);
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/*
		int fileSize = 10;
		String dbName = "Test";
		String datasetName = "c:\\users\\dpras\\tempdata\\testDouble_1.FC";
		
		FactDataWriterDouble t1 = null;
		FactDataWriterDouble t2 = null;
		
		try {
			
			long beginTime = System.nanoTime();
			int count = fileSize/2;
			
			int[] positions1 = new int[count+1];
			double[] values1 = new double[count+1];
			
			for (int i = 0; i <= count; i++) {
				positions1[i] = i;
				values1[i] = Double.MAX_VALUE - (i * 100);
			}
			
			int[] positions2 = new int[count+1];
			double[] values2 = new double[count+1];
			
			for (int i = 0; i <= count; i++) {
				positions2[i] = i+4;
				values2[i] = 7000000.001;
			}
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			beginTime = System.nanoTime();
			
			// create the segment
			t1 = new FactDataWriterDouble (dbName, datasetName, fileSize, 1);
			t1 = null;
			
			// write two threads into the segment to see if locking is working
			t1 = new FactDataWriterDouble (dbName, datasetName, HASIDSConstants.OPERATION_MODE_ONLINE);
			t2 = new FactDataWriterDouble (dbName, datasetName, HASIDSConstants.OPERATION_MODE_ONLINE);
			
			// set the write positions of the first thread
			t1.setWriteDataPositionBuffer(positions1, values1, false, false);
			
			// set the write positions of the second thread and force a retry
			t2.setWriteDataPositionBuffer(positions2, values2, false, true);
			
			// write the data in the first thread
			t1.writeToSegment(false);
			
			
			// sleep for 5 seconds
			//Thread.sleep(30000);
			
			// write the data in the second thread
			t2.writeToSegment(false);
						
			// commit the threads
			t1.commit();
			t2.commit();
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total program time : " + diff);
			
			
			
		}
		catch(Exception e) {
			
			try {
				t1.commit();
				t2.commit();
			}
			catch (Exception e1) {
				e.printStackTrace();
			}
			
			
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		*/
		
		int fileSize = 10;
		String dbName = "Test";
		String datasetName1 = "c:\\users\\dpras\\tempdata\\testDouble1_1.FC";
		String datasetName2 = "c:\\users\\dpras\\tempdata\\testDouble2_1.FC";
		
		FactDataWriterDouble t1 = null;
		FactDataWriterDouble t2 = null;
		
		try {
			
			long beginTime = System.nanoTime();
			
			int[] positions1 = new int[fileSize];
			double[] values1 = new double[fileSize];
			
			for (int i = 0; i < fileSize; i++) {
				positions1[i] = i;
				values1[i] = Double.MAX_VALUE - (i * 100);
			}
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			beginTime = System.nanoTime();
			
			// create the segment
			t1 = new FactDataWriterDouble (dbName, datasetName1, fileSize, 1);
			t2 = new FactDataWriterDouble (dbName, datasetName2, fileSize, 1);
			t1 = null;
			t2 = null;
			
			// write two threads into the segment to see if locking is working
			t1 = new FactDataWriterDouble (dbName, datasetName1, HASIDSConstants.OPERATION_MODE_ONLINE);
			t2 = new FactDataWriterDouble (dbName, datasetName2, HASIDSConstants.OPERATION_MODE_ONLINE);
			
			// set the write positions of the first thread
			t1.setWriteDataPositionBuffer(positions1, values1, false, false);
			
			// set the write positions of the second thread and force a retry
			t2.setWriteDataPositionBuffer(positions1, values1, false, false);
			
			// write the data in the first thread
			t1.writeToSegment(false);
			t2.writeToSegment(false);
						
			// commit the threads
			t1.commit();
			t2.commit();
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total program time : " + diff);
			
		}
		catch(Exception e) {
			
			try {
				t1.commit();
				t2.commit();
			}
			catch (Exception e1) {
				e.printStackTrace();
			}
			
			
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}
