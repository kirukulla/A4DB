package com.hasids.io.fact;

import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataWriter;

public class FactDataWriterString extends DataWriter {

	public FactDataWriterString(String dbName, String datasetName, int operationMode) throws Exception {
		super(dbName, datasetName, operationMode);
		// TODO Auto-generated constructor stub
	}

	public FactDataWriterString(String dbName, String datasetName, int recordCount, int recordLength, int segmentNo) throws Exception {
		super(dbName, datasetName, recordCount, CheckSum.FACT_ENCODE_TYPE_ALPHAN, recordLength, (short)0, segmentNo);
		// TODO Auto-generated constructor stub
	}
	
	public Hashtable<Integer, String> setWriteDataPositionBuffer(int[] position, String[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		return super.setWriteDataPositionBuffer(position, values, allowPartial, retryFlag);
	}
	
	public static Hashtable<Integer, String> getLockedKeysString(String dbName, String datasetName) {
		return DataWriter.getLockedKeysString(dbName, datasetName);
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//String testString = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
		String testString = "AAAAAAAAAAAAAAAAAAAA";
		int fileSize = 50000000;
		int dataLength = 40;
		String dbName = "Test";
		String datasetName = "c:\\users\\dpras\\tempdata\\testString_1.FC";
		
		FactDataWriterString t1 = null;
		//FactDataWriterString t2 = null;
		
		try {
			
			long beginTime = System.nanoTime();
			int count = 50000000;
			
			int[] positions1 = new int[count];
			String[] values1 = new String[count];
			
			for (int i = 0; i < count; i++) {
				positions1[i] = i;
				//values1[i] = (short)(((Math.random() * 1000000) % Short.MAX_VALUE));
				values1[i] = testString;
				//values1[i] = testString;
			}
			
			//int[] positions2 = new int[count];
			//String[] values2 = new String[count];
			
			/*for (int i = 0; i < count; i++) {
				positions2[i] = i+20;
				//values2[i] = (short)(((Math.random() * 1000000) % Short.MAX_VALUE));
				values2[i] = (7000000.001 + i) + "";
			}*/
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			//beginTime = System.nanoTime();
			
			// create the segment
			//t1 = new FactDataWriterString (dbName, datasetName, fileSize, dataLength, 1);
			
			// write two threads into the segment to see if locking is working
			t1 = new FactDataWriterString (dbName, datasetName, HASIDSConstants.OPERATION_MODE_BATCH);
			//t2 = new FactDataWriterString (dbName, datasetName, HASIDSConstants.OPERATION_MODE_ONLINE);
			
			// set the write positions of the first thread
			t1.setWriteDataPositionBuffer(positions1, values1, false, false);
			
			// set the write positions of the second thread and force a retry
			//t2.setWriteDataPositionBuffer(positions2, values2, false, true);
			
			// write the data in the first thread
			t1.writeToSegment(false);
			
			
			// sleep for 5 seconds
			//Thread.sleep(30000);
			
			// write the data in the second thread
			//t2.writeToSegment(false);
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total program time Before commit: " + diff);
			
			// commit the threads
			t1.commit();
			//t2.commit();
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Total program time : " + diff);
			
			
			
		}
		catch(Exception e) {
			
			try {
				t1.commit();
				//t2.commit();
			}
			catch (Exception e1) {
				e.printStackTrace();
			}
			
			
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		
	}

}
