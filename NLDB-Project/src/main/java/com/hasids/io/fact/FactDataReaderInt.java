package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderInt extends DataReader {

	public FactDataReaderInt() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderInt(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderInt(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderInt(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderInt(String dbName, String datasetName, int lowRange, int highRange, 
			short segmentNo, short dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_INT, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public int[] getFilter() {
		//return this._filterByte;
		return super.getFilterInt();
	}
	
	public BitSet getData(int[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(int[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public int[] getDataValues(int[] positions) throws Exception {
		int[] returnValue = (int[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(int[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(int[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(int c1, int c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(int c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(int c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(int c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(int c) throws Exception {
		super.setLTFilter(c);
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

