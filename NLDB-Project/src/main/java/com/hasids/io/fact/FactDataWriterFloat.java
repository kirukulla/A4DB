package com.hasids.io.fact;

import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataWriter;

public class FactDataWriterFloat extends DataWriter {

	public FactDataWriterFloat(String dbName, String datasetName, int operationMode) throws Exception {
		super(dbName, datasetName, operationMode);
		// TODO Auto-generated constructor stub
	}

	public FactDataWriterFloat(String dbName, String datasetName, int recordCount, int segmentNo) throws Exception {
		super(dbName, datasetName, recordCount, CheckSum.FACT_ENCODE_TYPE_FLOAT, 4, (short)0, segmentNo);
		// TODO Auto-generated constructor stub
	}
	
	public Hashtable<Integer, Float> setWriteDataPositionBuffer(int[] position, float[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		return super.setWriteDataPositionBuffer(position, values, allowPartial, retryFlag);
	}
	
	public static Hashtable<Integer, Float> getLockedKeysFloat(String dbName, String datasetName) {
		return DataWriter.getLockedKeysFloat(dbName, datasetName);
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int fileSize = 500000000;
		String dbName = "Test";
		String datasetName = "c:\\users\\dpras\\tempdata\\testFloat_2.FC";
		
		FactDataWriterFloat t1 = null;
		
		try {
			
			long beginTime = System.nanoTime();
			int count = fileSize;
			
			int[] positions = new int[count];
			float[] values = new float[count];
			
			for (int i = 0; i < count; i++) {
				positions[i] = i;
				values[i] = i%100 + 0.1f;
			}
			
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			beginTime = System.nanoTime();
			
			// create the segment
			t1 = new FactDataWriterFloat (dbName, datasetName, fileSize, 2);
			
			// set the write positions of the first thread
			t1.setWriteDataPositionBuffer(positions, values, false, false);
			
			// write the data in the first thread
			t1.writeToSegment(true);
			
			// commit the threads
			//t1.commit();
			
			
		}
		catch(Exception e) {
			
			try {
				t1.rollback();
			}
			catch (Exception e1) {
				e.printStackTrace();
			}
			
			
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		
	}

}
