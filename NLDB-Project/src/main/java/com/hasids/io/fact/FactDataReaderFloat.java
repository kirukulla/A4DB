package com.hasids.io.fact;

import java.util.BitSet;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.io.DataReader;

public final class FactDataReaderFloat extends DataReader {

	public FactDataReaderFloat() {
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderFloat(String dbName, String datasetName) throws Exception {
		super(dbName, datasetName);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderFloat(String dbName, String datasetName, int lowRange) throws Exception {
		super(dbName, datasetName, lowRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderFloat(String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		super(dbName, datasetName, lowRange, highRange);
		// TODO Auto-generated constructor stub
	}

	public FactDataReaderFloat(String dbName, String datasetName, int lowRange, int highRange, 
			short segmentNo, short dataLength, short decimals) throws Exception {
		super(dbName, datasetName, lowRange, highRange, CheckSum.FILE_TYPE_FACT, CheckSum.FACT_ENCODE_TYPE_FLOAT, segmentNo, dataLength, decimals);
		// TODO Auto-generated constructor stub
	}
	
	public float[] getFilter() {
		//return this._filterByte;
		return super.getFilterFloat();
	}
	
	public BitSet getData(float[] filter) {
		return super.getData(filter, false);
	}
	
	public BitSet getData(float[] filter, boolean not) {
		return super.getData(filter, not);
	}
	
	public float[] getDataValues(int[] positions) throws Exception {
		float[] returnValue = (float[])super.getDataValues(positions);
		
		return returnValue;
	}
	
	public void setFilter(float[] c) throws Exception {
		super.setFilter(c, false);
	}
	
	public void setFilter(float[] c, boolean not) throws Exception {
		super.setFilter(c, not);
	}
	
	public void setBETWEENFilter(float c1, float c2) throws Exception {
		super.setBETWEENFilter(c1, c2);
	}
	
	public void setGTEQFilter(float c) throws Exception {
		super.setGTEQFilter(c);
	}
	
	public void setGTFilter(float c) throws Exception {
		super.setGTFilter(c);
	}

	public void setLTEQFilter(float c) throws Exception {
		super.setLTEQFilter(c);
	}
	
	public void setLTFilter(float c) throws Exception {
		super.setLTFilter(c);
	}
	
	public Hashtable<Integer, ImmutableRoaringBitmap> readFloatValuesAsGroupedInt (int noParallelReadThreads) throws Exception {
		return super.readFloatValuesAsGroupedInt(noParallelReadThreads);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}

