package com.hasids.tests;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hasids.io.dim.DimDataReader;

public class TestDimDataValues {
	private static final Logger logger = LoggerFactory.getLogger(TestDimDataValues.class);
	public TestDimDataValues() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		logger.info("Testing SLF4J API Service for Logger");
		// TODO Auto-generated method stub
		// EXAMPLE TO GET DATA VALUES 
		
		try {
			long beginTime = System.nanoTime();
			
			int lowRange = 1;
			int highRange = 100000000;
			int noPositions = highRange - lowRange + 1;
			
			int positions[] = new int[noPositions];
			for (int i = 0; i < noPositions; i++)
				positions[i] = i + 1;
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			DimDataReader hr = new DimDataReader(dbName, datasetName, lowRange, highRange);
			logger.info("datasetName: " + hr.getdatasetName());
			int[] values = hr.getDataValues(positions);
			
			logger.info("Returned values : " + values.length);
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			logger.info("Time to retrieve data into memory in millis (BitSet) : " + diff);
			
			for (int i = 0; i < noPositions; i=i+1000000)
				logger.info("Position (" + positions[i] + "), Value (" + i + ") : " + values[i]);
			
			
		}
		catch (Exception e) {
			;
		}
		
	}

}
