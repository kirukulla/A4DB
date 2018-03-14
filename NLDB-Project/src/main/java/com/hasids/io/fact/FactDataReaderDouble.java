package com.hasids.io.fact;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderDouble extends DataReader {

	public FactDataReaderDouble() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderDouble(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderDouble(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderDouble(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderDouble(String dbName, String datasetName, int lowRange, int highRange, 
			short segmentNo, short dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_DOUBLE, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public double[] getFilter() {
		//return this._filterByte;
		return super.getFilterDouble();
	}
	
	public BitSet getData(double[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(double[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public double[] getDataValues(int[] positions) throws Exception {
		double[] returnValue = (double[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(double[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(double[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(double c1, double c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(double c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(double c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(double c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	protected void setLTFilter(double c) throws Exception {
		super.setLTFilter(c);
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

