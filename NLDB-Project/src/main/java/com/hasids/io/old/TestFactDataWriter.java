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
 * This class is a test fact writer component. It allows the creation of fact data segments 
 * and writing fact data to the data segments. File sizes are restricted to 2 billion facts.
 * 
 * For the write to work correctly, the position array must be sorted in ascending order
 * to ensure all writes are complete in a single parse from the lowest position to the
 * highest position
 * 
 * Another approach to conserve memory is to have a write function with the input containing 
 * a byte array of length that is the difference of highest position minus lowest position + 1.
 * This array can be read positionally till a position equal to INTEGER.MAX_VALUE and iteratively 
 * setting values till the highest position is reached. Therefore to write a fact we will need 
 * only the fact values positioned correctly. 
 */
public class TestFactDataWriter implements Runnable {
	
	private String _datasetName;
	private int _recordCount;
	private short _valueLength; // value length includes the decimal portion
	private short _decimals;
	private RandomAccessFile _randomAccessFile;
	long[] _position; //position
	double[] _values; // values
	
	private int _status = HASIDSConstants.THREAD_INACTIVE;
	
	public TestFactDataWriter(String datasetName) throws Exception {
		// TODO Auto-generated constructor stub
		this._datasetName = datasetName;
		try {
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			this._recordCount = (int)f.length();
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	public TestFactDataWriter(String datasetName, int recordCount, short valueLength, short decimals) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= 2 billion");
		
		this._recordCount = recordCount;
		this._valueLength = valueLength;
		this._decimals = decimals;
		
		try {
			this.createSegment();
		}
		catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	public String getDatasetName() {
		return this._datasetName;
	}
	
	public int getRecordCount() {
		return this._recordCount;
	}
	
	private void setWriteDataPositionBuffer(long[] position) throws Exception {
		
		for (int i = 0; i < position.length; i++)
		{
			if (position[i] < 0 || position[i] > this._recordCount - 1)
				throw new Exception ("Positions to be set should be >= 0 and <= file length/record count");
		}
		
		this._position = position;
	}
	
	private void setWriteDataValues(double[] values) throws Exception {
		
		this._values = values;
	}
	
	public void setDataToWrite (long[] position, double[] values) throws Exception {
		this.setWriteDataPositionBuffer(position);
		this.setWriteDataValues(values);
	}
	
	/**
	 * Create a segment matching the number of records using the fact length / value length
	 * as the basis of calculating the actual data size. Each byte is written as zero. This 
	 * way when the fact file is read, the entire value corresponding to a record key can
	 * be read in one instance without the worry of any conversion.
	 * 
	 * @throws Exception
	 */
	private void createSegment() throws Exception {
		this._status = HASIDSConstants.THREAD_ACTIVE;
		byte buf = 48;
		FileChannel rwChannel = null;
		
		long beginTime = System.nanoTime();
		
		try {
			File f = new File(this._datasetName);
			if (f.exists())
				throw new Exception("File " + this._datasetName + " exists! Cannot create segment");
			System.out.println("ValueLength / recordCount : " + _valueLength + " / " + _recordCount);
			long fileSize = (long)_valueLength * (long)_recordCount;
			System.out.println("File size : " +  fileSize);
			int mapSize = 2000000000; // max no of bytes we can map at a time
			if (mapSize > fileSize) // if our filesize is less that allowable map size, then we map to file size
				mapSize = (int) fileSize;
			
			long noIterations = fileSize/mapSize; // no of iterations of file mapping with each mapping equal to 2 billion bytes
			long remaining =  fileSize % mapSize; // if the division is not even
			System.out.println("File size / Map size / no Iterations / no remaining : " + fileSize + " / " + mapSize + " / " + noIterations + " / " + remaining);
			
			_randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			rwChannel = _randomAccessFile.getChannel();
			
			long start = 0;
			ByteBuffer buffer = null;
			for (int j = 1; j <= noIterations; j++) {
				System.out.println("Start Position : " + start);
				buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, start, mapSize);
				for (long i = start; i < mapSize; i++)
				{
					buffer.put(buf);
				}
				start += mapSize;
				
			}
			
			System.out.println("File length = " + _randomAccessFile.length());
			rwChannel.close();
			_randomAccessFile.close();
			
		}
		catch(Exception e) {
			this._status = HASIDSConstants.THREAD_FAILED;
			//throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == HASIDSConstants.THREAD_ACTIVE)
				this._status = HASIDSConstants.THREAD_COMPLETE;
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("Segment creation time for : " + this._recordCount + " = " + elapsedTimeInMillis + "  Milliseconds");
            
			try {
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
	
	public void writeToSegment() throws Exception {
		
		this._status = HASIDSConstants.THREAD_ACTIVE;
		
		FileChannel rwChannel = null;
		long beginTime = System.nanoTime();
		
		try {
			if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
				throw new Exception("Null Data or data length does not match values length! No data to write");
			
			
			
			File f = new File(this._datasetName);
			if (!f.exists())
				this.createSegment();
			
			long value;
			_randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			rwChannel = _randomAccessFile.getChannel();
			MappedByteBuffer buffer = null;
			String tmp = null;
			int tempLen = 0;
			
			// each run is 5 updates from Main Function
			// repeat this 10000 times to create a 50000 transaction set
			for (int i = 0; i < _position.length; i++)
			{
				// get the values to be written into the buffer and convert to non-decimal
				value = (long)(_values[i] * Math.pow(10, _decimals)); // convert double to long while eliminating decimals
				//Double.doubleToLongBits(value);
				
				tmp = Long.toString(value);
				
				//System.out.println("String Buffer 1 : " + temp + ", Length = " + temp.length());
				tempLen = tmp.length();
				
				// Map buffer into Memory with adjusted position from position of unary key
				buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, (_position[i] * this._valueLength) + (_valueLength - tempLen), this._valueLength);
				
				buffer.put(tmp.getBytes());
			}
			
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
			this.writeToSegment();
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		try {
			System.out.println("Max Integer Value = " + Integer.MAX_VALUE);
			System.out.println("Max Long Value = " + Long.MAX_VALUE);
			
			
			int count = 100000;
			long[] position = new long[count];
			double[] values = new double[count];
			
			for (int i = 0; i < count; i++) {
				position[i] = i;
				values[i] = i;
			}
			
			
			TestFactDataWriter t = new TestFactDataWriter ("c:\\users\\dpras\\tempdata\\ordervalue.FC",count,(short)16,(short)2);
			t.setDataToWrite(position, values);
			t.writeToSegment();
			
			/*data = new long[] {100000,1000000,100000000,200000000,400000000}; //position
			values = new double[] {10,72,73,74,75}; // values
			
			t.setWriteDataPositionBuffer(data);
			t.setWriteDataValues(values);
			t.writeToSegment();*/
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
