package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderByte extends DataReader {

	public FactDataReaderByte() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderByte(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderByte(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderByte(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderByte(String dbName, String datasetName, int lowRange, int highRange, 
			int segmentNo, int dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_BYTE, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public byte[] getFilter() {
		//return this._filterByte;
		return super.getFilterByte();
	}
	
	public BitSet getData(byte[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(byte[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public byte[] getDataValues(int[] positions) throws Exception {
		byte[] returnValue = (byte[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(byte[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(byte[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(byte c1, byte c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(byte c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(byte c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(byte c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(byte c) throws Exception {
		super.setLTFilter(c);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

