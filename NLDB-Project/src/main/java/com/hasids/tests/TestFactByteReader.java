package com.hasids.tests;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.io.fact.FactDataReaderByte;

public class TestFactByteReader {

	public TestFactByteReader() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int lowRange = 1;
		int highRange = 100;
		
		byte[] c = {80, 15, 26, 30};
		int[] positions = {1,11,21,31,41,51,61,71,81,91};
		
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long diff = 0;
		
		try {
			
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testByte_1.FC";
			FactDataReaderByte hr = new FactDataReaderByte(dbName, datasetName, lowRange, highRange);
			//hr.setGTEQFilter(c[0]);
			//hr.setLTEQFilter(c[1]);
			//hr.setBETWEENFilter(c[2], c[3]);
			System.out.println("datasetName: " + hr.getdatasetName());
			BitSet result = hr.getData(c);
			System.out.println("Cardinality 0 : " + result.cardinality());
			
			
			int[] list = hr.getResultSetArray();
			System.out.println("No of records in result BitSet : " + list.length);
			
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("File read time in millis : " + hr.getElapsedTime());
					
			//System.out.println("No of records in result BitSet : " + k);
			System.out.println("Operations time in memory in millis : " + diff);	
			
			byte[] values = hr.getDataValues(positions);
			if (values != null) {
				System.out.println("No of Returned values : " + values.length);
				for(int i = 0; i < positions.length; i++)
					System.out.println("Position : " + positions[i] + ", value : " + values[i]);
			}
			else
				System.out.println("No data values returned");
			
		}
		catch (Exception e ) {
			e.printStackTrace();
		}

	}

}
