package com.hasids.tests;

import java.util.Hashtable;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.hasids.io.fact.FactDataReaderFloat;

public class TestFloatToIntGrouping {

	public TestFloatToIntGrouping() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String dbName = "Test";
		String datasetName = "c:\\users\\dpras\\tempdata\\testFloat_1.FC";
		
		try {
			FactDataReaderFloat reader = new FactDataReaderFloat(dbName, datasetName);
			Hashtable<Integer, ImmutableRoaringBitmap> h = reader.readFloatValuesAsGroupedInt(400);
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}

}
