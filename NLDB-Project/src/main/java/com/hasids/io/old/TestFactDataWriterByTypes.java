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
import java.util.Hashtable;
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
public class TestFactDataWriterByTypes implements Runnable {
	
	private String _datasetName;
	private int _recordCount;
	private short _dataType; // value length includes the decimal portion
	private RandomAccessFile _randomAccessFile;
	private short _valueLength;
	long[] _position; //position
	double[] _values; // values
	
	private int _status = HASIDSConstants.THREAD_INACTIVE;
	
	public TestFactDataWriterByTypes(String datasetName) throws Exception {
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
	
	public TestFactDataWriterByTypes(String datasetName, int recordCount, short dataType) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= 2 billion");
		
		this._recordCount = recordCount;
		
		if (dataType == HASIDSConstants.DATA_TYPE_FLOAT)
			this._valueLength = 10;
		else if	(dataType == HASIDSConstants.DATA_TYPE_DOUBLE)
			this._valueLength = 19;
		else
			throw new Exception ("Invalid data type.....!");
		
		this._dataType = dataType;
		
		try {
			this.createSegment();
		}
		catch (Exception e) {
			;
			//throw new Exception(e.getMessage());
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
		
		if (this._position.length != values.length)
			throw new Exception ("Mismatch between data position and data value buffers!");
		
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
			
			// if we are storing a float - 10 digits, we can evenly map at 2 billiion (200 million records)
			// if we are storing a double - 19 digit, we can map evenly at 1.9 bilion (100 million records)
			int mapSize = HASIDSConstants.FACT_FLOAT_MAX_BYTES; // max no of bytes we can map at a time
			
			if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE)
				mapSize = HASIDSConstants.FACT_DOUBLE_MAX_BYTES;
			
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
			
			// Account for the remaining bytes
			if (remaining > 0) {
				buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, start, remaining);
				for (long i = start; i < mapSize; i++)
				{
					buffer.put(buf);
				}
			}
			
			System.out.println("File length = " + _randomAccessFile.length());
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
		long endTime = System.nanoTime();
		long position = -1;
		
		try {
			if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
				throw new Exception("Null Data or data length does not match values length! No data to write");
			
			
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception ("File " + this._datasetName + " does not exist!");
			
			long fileSize = (long)_valueLength * (long)_recordCount;
			System.out.println("File size : " +  fileSize);
			
			long value;
			
			// if we are storing a float - 10 digits, we can evenly map at 2 billiion (200 million records)
			// if we are storing a double - 19 digit, we can map evenly at 1.9 bilion (100 million records)
			int mapRecordCount = HASIDSConstants.FACT_FLOAT_MAX_RECORDS;
			int mapSize = HASIDSConstants.FACT_FLOAT_MAX_BYTES; // max no of bytes we can map at a time
						
			if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
				mapSize = HASIDSConstants.FACT_DOUBLE_MAX_BYTES;
				mapRecordCount = HASIDSConstants.FACT_DOUBLE_MAX_RECORDS;
			}
			
			if (mapSize > fileSize) {// if our filesize is less that allowable map size, then we map to file size
				mapSize = (int) fileSize;
				mapRecordCount = (int) fileSize/this._valueLength;
			}
			
			System.out.println("Map Size : " + mapSize);
			
						
			long noIterations = fileSize/mapSize; // no of iterations of file mapping with each mapping equal to 2 billion bytes
			long remaining =  fileSize % mapSize; // if the division is not even
			//System.out.println("File size / Map size / no Iterations / no remaining : " + fileSize + " / " + mapSize + " / " + noIterations + " / " + remaining);
			
			_randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			rwChannel = _randomAccessFile.getChannel();
			
			
			// create a hashtable to store the start positions and the corresponding byte buffers 
			Hashtable<Integer, MappedByteBuffer> h = new Hashtable<Integer, MappedByteBuffer>(); 
			
			// identify the boundaries for each iteration and store in into a hashtable to be retrieved later
			long start = 0;
			
			// map the file portions to buffers in memory
			int j = 0;
			for (j = 0; j < noIterations; j++) {
				System.out.println("Start Position : " + start + ", Hashtable Key : " + j);
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, start, mapSize);
				h.put(j, buffer);
				start += mapSize;
			}
			
			if (remaining > 0) {
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, start, remaining);
				h.put(j, buffer);
			}
			
			System.out.println("No of entries in Hashtable : " + h.size());
			endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("File mapping time : " + diff + "  Milliseconds");
			
            beginTime = System.nanoTime();
			String tmp = null;
			
			MappedByteBuffer buffer = null;
			for (int i = 0; i < _position.length; i++)
			{
				
				//position = (_position[i]%mapRecordCount * this._valueLength);
				//System.out.println("Setting position : " + _position[i] + " / " + position);
				//System.out.println("hashtable key : " + (_position[i] * this._valueLength)/mapSize);
				
				// get the correct buffer for the given position that data must be written
				buffer = h.get((int) _position[i]/mapSize);
				
				// if we don't find the buffer, break
				// this should never happen, just in case
				if (buffer == null) {
					this._status = HASIDSConstants.THREAD_FAILED;
					break;
				}
				
				//System.out.println("Buffer found!");
				// convert the value into either Integer bits or Long bits and then to raw bytes
				// to be written
				if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT) {
					if (_values[i] == 0)
						tmp = HASIDSConstants.FACT_FLOAT_ZERO_STRING;
					else
						tmp = Integer.toString(Float.floatToIntBits((float)_values[i]));
				}
				else if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
					if (_values[i] == 0)
						tmp = HASIDSConstants.FACT_DOUBLE_ZERO_STRING;
					else
						tmp = Long.toString(Double.doubleToLongBits(_values[i]));
				}
				
				//System.out.println("Value being set = " + tmp);
				// Map buffer into Memory with adjusted position from position of unary key
				buffer.position((int) (_position[i]%mapRecordCount * this._valueLength));
				//System.out.println("Position set in buffer");
				buffer.put(tmp.getBytes());
				
				// flush every 50000 records
				//if (i%1000000 == 0)
				//	buffer.force();
				
			}
			
			//buffer.force();
			
			rwChannel.close();
			_randomAccessFile.close();
			
			//System.out.println("Count : " + count + ", Position : " + position);
		}
		catch(Exception e) {
			System.out.println("Failure in write at position : " + position);
			this._status = HASIDSConstants.THREAD_FAILED;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == HASIDSConstants.THREAD_ACTIVE)
				this._status = HASIDSConstants.THREAD_COMPLETE;
			
			try {	
				endTime = System.nanoTime();
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
			TestFactDataWriterByTypes t = new TestFactDataWriterByTypes (
					"c:\\users\\dpras\\tempdata\\ov2.FC", 200000000, HASIDSConstants.DATA_TYPE_FLOAT);
			
			int count = 200000000;
			
			long[] position = new long[count];
			double[] values = new double[count];
			
			float f = 0.01f;
			
			for (int i = 0; i < count; i++) {
				position[i] = i;
				values[i] = 1.0 + ((float)f * (float)i);
			}
			
			t.setDataToWrite(position, values);
			t.writeToSegment();
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
