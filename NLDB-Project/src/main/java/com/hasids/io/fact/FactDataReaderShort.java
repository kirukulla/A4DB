package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderShort extends DataReader {

	public FactDataReaderShort() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderShort(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderShort(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderShort(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderShort(String dbName, String datasetName, int lowRange, int highRange, 
			int segmentNo, int dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_SHORT, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public short[] getFilter() {
		//return this._filterByte;
		return super.getFilterShort();
	}
	
	public BitSet getData(short[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(short[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public short[] getDataValues(int[] positions) throws Exception {
		short[] returnValue = (short[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(short[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(short[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(short c1, short c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(short c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(short c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(short c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(short c) throws Exception {
		super.setLTFilter(c);
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

