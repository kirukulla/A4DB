/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.old;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 * This class is a test fact reader component. It allows the read of facts for a given
 * set of ids.
 * 
 * The technique used will be to map the file to memory and then do positional reading. 
 */
public class TestFactDataReader implements Runnable {
	
	private String _datasetName;
	private long _recordCount;
	private short _valueLength; // value length includes the decimal portion
	private short _decimals;
	private RandomAccessFile _randomAccessFile;
	private int[] _positions;
	private double[] _values;
	
	private int _status = HASIDSConstants.THREAD_INACTIVE;
	
	public TestFactDataReader(String datasetName, short valueLength, short decimals) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		try {
			File f = new File(datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			System.out.println("File size = " + f.length());
			this._recordCount = f.length()/valueLength;
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
		}
		
		this._datasetName = datasetName;
		if (this._recordCount <= 0 || this._recordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion records
			throw new Exception ("Record count must be > 0 and <= 2 billion");
		
		System.out.println("Record count in file : " + this._recordCount);
		if (decimals > valueLength)
			throw new Exception ("No of decimals cannot exceed the length of the decimal number!");
		
		this._valueLength = valueLength;
		this._decimals = decimals;
		
	}
	
	public void setDataReadPositions(int[] positions) throws Exception {
		if (positions == null || positions.length <= 0)
			throw new Exception ("Array of positions cannot be null or empty!");
		
		for (int i = 0; i < positions.length; i++)
		{
			if (positions[i] < 0 || positions[i] > this._recordCount - 1)
				throw new Exception ("Positions to be set should be >= 0 and <= file length/record count");
		}
		
		this._positions = positions;
		
		// initialize the values buffer
		this._values = new double[positions.length];
	}
	
	public String getDatasetName() {
		return this._datasetName;
	}
	
	public long getRecordCount() {
		return this._recordCount;
	}
	
	public short getDataLength() {
		return this._valueLength;
	}
	
	public short getDecimalPositions() {
		return this._decimals;
	}
	
	public double[] getValues() {
		return this._values;
	}
	
	public void getFactData( ) throws Exception {
		
		this._status = HASIDSConstants.THREAD_ACTIVE;
		
		FileChannel rwChannel = null;
		long beginTime = System.nanoTime();
		
		try {
			
			// check if file exists
			
			_randomAccessFile = new RandomAccessFile(this._datasetName, "r");
			rwChannel = _randomAccessFile.getChannel();
			
			// we can map up to 2 billion bytes at a time (2.14 billion actually which is
			// equivalent to INTEGER.MAX_VALUE, however to keep things simple, in HASIDS 
			// we cap a segment size to 2 billion. if the file length is > 2 billion, we will 
			// have to map by blocks of 2 billion or a number that is a multiple of the length 
			// of the individual number and closest to 2 billion. 
			

			MappedByteBuffer buffer = null;
			
			byte[] dst = new byte[this._valueLength];
			
			// each run is 5 updates from Main Function
			// repeat this 10000 times to create a 50000 transaction set
			//for (int count = 0; count < 10000; count++) {
			for (int i = 0; i < this._positions.length; i++)
			{
				//System.out.println("Id = " + _positions[i]);
				//System.out.println("Position : " + (_positions[i] * this._valueLength));
				buffer = rwChannel.map(FileChannel.MapMode.READ_ONLY, ((long)_positions[i] * this._valueLength), this._valueLength);
				//System.out.println("Value = " + ByteBuffer.wrap(dst).getDouble());
				this._values[i] = (ByteBuffer.wrap(dst).getDouble()) / Math.pow(10, _decimals);
			}
			
			//}
			rwChannel.close();
			_randomAccessFile.close();
			
		}
		catch(Exception e) {
			this._status = HASIDSConstants.THREAD_FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == HASIDSConstants.THREAD_ACTIVE)
				this._status = HASIDSConstants.THREAD_COMPLETE;
			
			try {	
				long endTime = System.nanoTime();
				long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	            System.out.println("File based read/write time : " + elapsedTimeInMillis + "  Milliseconds");
	            
				if (rwChannel != null)
					rwChannel.close();
				if (_randomAccessFile != null)
					_randomAccessFile.close();
			}
			catch (Exception e) {
				throw new Exception(e.getMessage());
			}
			
		}
	}
	
	public void run() {
		// TODO Auto-generated method stub
		try {
			this.getFactData();
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		try {
			TestFactDataReader t = new TestFactDataReader ("c:\\users\\dpras\\tempdata\\ordervalue.FC",(short)16,(short)2);
			
			System.out.println("Max Integer = " + Integer.MAX_VALUE);
			int[] positions = {0,2,4,6,8,200,100,20000,99000}; //position
			t.setDataReadPositions(positions);
			t.getFactData();
			double[] values = t.getValues();
			System.out.println("Number of values received = " + values.length);
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
