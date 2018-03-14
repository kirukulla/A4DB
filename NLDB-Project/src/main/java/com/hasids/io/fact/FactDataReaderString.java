package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderString extends DataReader {

	public FactDataReaderString() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderString(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderString(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderString(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderString(String dbName, String datasetName, int lowRange, int highRange, 
			short segmentNo, short dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_ALPHAN, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public String[] getFilter() {
		//return this._filterByte;
		return super.getFilterString();
	}
	
	public BitSet getData(String[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(String[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public String[] getDataValues(int[] positions) throws Exception {
		String[] returnValue = (String[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(String[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(String[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(String c1, String c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(String c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(String c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(String c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(String c) throws Exception {
		super.setLTFilter(c);
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

