package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderLong extends DataReader {

	public FactDataReaderLong() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderLong(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderLong(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderLong(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderLong(String dbName, String datasetName, int lowRange, int highRange, 
			short segmentNo, short dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_LONG, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public long[] getFilter() {
		//return this._filterByte;
		return super.getFilterLong();
	}
	
	public BitSet getData(long[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(long[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public long[] getDataValues(int[] positions) throws Exception {
		long[] returnValue = (long[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(long[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(long[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(long c1, long c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(long c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(long c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(long c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(long c) throws Exception {
		super.setLTFilter(c);
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

