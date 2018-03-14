package com.hasids.io.fact;

import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataWriter;

public class FactDataWriterInt extends DataWriter {

	public FactDataWriterInt(String dbName, String datasetName, int operationMode) throws Exception {
		super(dbName, datasetName, operationMode);
		// TODO Auto-generated constructor stub
	}

	public FactDataWriterInt(String dbName, String datasetName, int recordCount, int segmentNo) throws Exception {
		super(dbName, datasetName, recordCount, CheckSum.FACT_ENCODE_TYPE_INT, 4, (short)0, segmentNo);
		// TODO Auto-generated constructor stub
	}
	
	public Hashtable<Integer, Integer> setWriteDataPositionBuffer(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		return super.setWriteDataPositionBuffer(position, values, allowPartial, retryFlag);
	}
	
	public static Hashtable<Integer, Integer> getLockedKeysInt(String dbName, String datasetName) {
		return DataWriter.getLockedKeysInt(dbName, datasetName);
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int fileSize = 10;
		String dbName = "Test";
		String datasetName = "D:\\Data\\TestDB\\TestCluster\\testInt_1.FC";
		
		FactDataWriterInt t1 = null;
		FactDataWriterInt t2 = null;
		
		try {
			
			long beginTime = System.nanoTime();
			int count = 6;
			
			int[] positions1 = new int[count];
			int[] values1 = new int[count];
			
			for (int i = 0; i < count; i++) {
				positions1[i] = i;
				//values1[i] = (short)(((Math.random() * 1000000) % Short.MAX_VALUE));
				values1[i] = 65;
			}
			
			int[] positions2 = new int[count];
			int[] values2 = new int[count];
			
			for (int i = 0; i < count; i++) {
				positions2[i] = i+4;
				//values2[i] = (short)(((Math.random() * 1000000) % Short.MAX_VALUE));
				values2[i] = 70;
			}
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			beginTime = System.nanoTime();
			
			// create the segment
			t1 = new FactDataWriterInt (dbName, datasetName, fileSize, 1);
			
			// write two threads into the segment to see if locking is working
			t1 = new FactDataWriterInt (dbName, datasetName, HASIDSConstants.OPERATION_MODE_ONLINE);
			t2 = new FactDataWriterInt (dbName, datasetName, HASIDSConstants.OPERATION_MODE_ONLINE);
			
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
