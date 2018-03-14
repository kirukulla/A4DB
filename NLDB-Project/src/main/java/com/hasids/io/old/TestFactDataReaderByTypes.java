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
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.HASIDSFactStructure;

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
public class TestFactDataReaderByTypes implements Runnable {
	
	private String _datasetName;
	private short _dataType = -1;
	private long _fileRecordCount;
	private short _valueLength; // value length includes the decimal portion
	
	// memory variable
	HASIDSFactStructure _hfs = new HASIDSFactStructure(); 
	
	// file variable
	private RandomAccessFile _randomAccessFile;
	
	// variables for positions and values for a regular read
	private int[] _positions;
	private double[] _values;
	
	// variables for multi threading
	private int _filterLowRange = 1; // for beginning of file, it must be set to 1
	private int _filterHighRange = 0; // high range - exclusive
	
	
	
	private int _status = HASIDSConstants.THREAD_INACTIVE;
	
	public TestFactDataReaderByTypes(String datasetName, short dataType) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		try {
			File f = new File(datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			System.out.println("File size = " + f.length());
			
			if (dataType == HASIDSConstants.DATA_TYPE_FLOAT)
				this._valueLength = (short)HASIDSConstants.FACT_FLOAT_ZERO_STRING.length();
			else if (dataType == HASIDSConstants.DATA_TYPE_DOUBLE)
				this._valueLength = (short)HASIDSConstants.FACT_DOUBLE_ZERO_STRING.length();
			else
				throw new Exception ("Invalid data type received!");
			
			this._fileRecordCount = f.length()/this._valueLength;
			this._dataType = dataType;
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
		}
		
		this._datasetName = datasetName;
		if (this._fileRecordCount <= 0 || this._fileRecordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion records
			throw new Exception ("Record count must be > 0 and <= 2 billion");
		
		System.out.println("Record count in file : " + this._fileRecordCount);
		
	}
	
	public void setDataReadPositions(int[] positions) throws Exception {
		if (positions == null || positions.length <= 0)
			throw new Exception ("Array of positions cannot be null or empty!");
		
		for (int i = 0; i < positions.length; i++)
		{
			if (positions[i] < 0 || positions[i] > this._fileRecordCount - 1)
				throw new Exception ("Positions to be set should be >= 0 and < file record count : " + this._fileRecordCount);
		}
		
		this._positions = positions;
		
		// initialize the values buffer
		this._values = new double[positions.length];
	}
	
	public void setFilterRanges(int lowFilterRange, int highFilterRange) throws Exception {
		if (lowFilterRange < 0 || highFilterRange < 0)
			throw new Exception("Filter ranges cannot be less than zero!");
		int diff = highFilterRange - lowFilterRange + 1;
		if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT && diff > HASIDSConstants.FACT_FLOAT_MAX_RECORDS)
			throw new Exception ("Max number of records to read for FLOAT data type is " + HASIDSConstants.FACT_FLOAT_MAX_RECORDS);
		if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE && diff > HASIDSConstants.FACT_DOUBLE_MAX_RECORDS)
			throw new Exception ("Max number of records to read for DOUBLE data type is " + HASIDSConstants.FACT_DOUBLE_MAX_RECORDS);
	}
	
	public String getDatasetName() {
		return this._datasetName;
	}
	
	public long getFileRecordCount() {
		return this._fileRecordCount;
	}
	
	public short getDataLength() {
		return this._valueLength;
	}
	
	public double[] getValues() {
		return this._values;
	}
	
	/**
	 * Method to load the file into memory
	 */
	public void loadToMemory() throws Exception {
		FileChannel rwChannel = null;
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long position = -1;
		
		// temporary structures initialized
		double[] d  = new double[0];
		float[] f = new float[0];
		
		try {

			long fileSize = (long)_valueLength * (long)_fileRecordCount;
			System.out.println("File size : " +  fileSize);
			
			
			// if we are storing a float - 10 digits, we can evenly map at 2 billiion (200 million records)
			// if we are storing a double - 19 digit, we can map evenly at 1.9 bilion (100 million records)
			int mapRecordCount = HASIDSConstants.FACT_FLOAT_MAX_RECORDS;
			int mapSize = HASIDSConstants.FACT_FLOAT_MAX_BYTES; // max no of bytes we can map at a time
						
			if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
				mapSize = HASIDSConstants.FACT_DOUBLE_MAX_BYTES;
				mapRecordCount = HASIDSConstants.FACT_DOUBLE_MAX_RECORDS;
				d = new double[(int) this._fileRecordCount];
			}
			else if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT) {
				f = new float[(int) this._fileRecordCount];
			}
			
			if (mapSize > fileSize) {// if our filesize is less that allowable map size, then we map to file size
				mapSize = (int) fileSize;
				mapRecordCount = (int) fileSize/this._valueLength;
			}
			
			System.out.println("Map Size : " + mapSize);
			
			long noIterations = fileSize/mapSize; // no of iterations of file mapping with each mapping equal to 2 billion bytes
			int remaining =  (int) fileSize % mapSize; // if the division is not even
			//System.out.println("File size / Map size / no Iterations / no remaining : " + fileSize + " / " + mapSize + " / " + noIterations + " / " + remaining);
			
			_randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			rwChannel = _randomAccessFile.getChannel();
			
			
			// identify the boundaries for each iteration and store in into a hashtable to be retrieved later
			long start = 0;
			
			// map the file portions to buffers in memory
			int j = 0;
			int count = 0;
			byte[] dst = new byte[10];
			if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE)
				dst = new byte[19];
			
			for (j = 0; j < noIterations; j++) {
				System.out.println("Start Position : " + start + ", Hashtable Key : " + j);
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_ONLY, start, mapSize);
				
				// start reading in the bytes, convert them into a String, then to a Double
				// and then back to its String representation.
				
				int limit = buffer.limit()/this._valueLength;
				for (int i = 0; i < limit; i++) { // 200 millis for 200M
					
					for (int k = 0; k < this._valueLength; k++) // 2600 millis for 200M
				         dst[k] = buffer.get();
					
					if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT) {
						f[count] = Float.intBitsToFloat(Integer.parseInt(new String(dst)));
						//if (i < 100 || (i > 199999900 && i < 200000000) )
						//	System.out.println(f[i]);
					}
					else if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
						d[count] = Double.longBitsToDouble(Long.parseLong(new String(dst)));
						//if (i < 100)
						//	System.out.println(d[count]);
					}
					
					++count;
					
				}
				
				buffer = null;
				System.out.println("Record Count : " + count);
			}
			
			if (remaining > 0) {
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_ONLY, start, remaining);
				int limit = buffer.limit()/this._valueLength;
				for (int i = 0; i < limit; i++) { // 200 millis for 200M
					
					for (int k = 0; k < this._valueLength; k++) // 2600 millis for 200M
				         dst[k] = buffer.get();
					
					// 17000 millis for 200M for float
					if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT) {
						f[count] = Float.intBitsToFloat(Integer.parseInt(new String(dst)));
						//if (i < 100 || (i > 199999900 && i < 200000000) )
						//	System.out.println(f[i]);
					}
					else if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
						d[count] = Double.longBitsToDouble(Long.parseLong(new String(dst)));
						//if (i < 100 || (i > 199999900 && i < 200000000))
						//	System.out.println(d[count]);
					}
					
					++count;
					
				}
			}
			
			// assign the values to memory storage
			if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT)
				this._hfs.setFloatValues(f);
			else if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE)
				this._hfs.setDoubleValues(d);
			
			//System.out.println("No of entries in Hashtable : " + this._memoryTable.size());
			endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("File mapping and copying time : " + diff + "  Milliseconds");
			
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
	            System.out.println("Memory copy time : " + elapsedTimeInMillis + "  Milliseconds");
	            
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
	
	/**
	 * This method is used to get fact data for a given set of ids. These ids must first be
	 * set after the class instance is created using the setDatareadPositions method and are 
	 * stored in an instance variable. The matching facts are stored in an instance variable 
	 * which are retrieved using the getValues method. This method is always run as a single
	 * thread. This method is for getting facts only, the caller assumes the responsibility
	 * of doing any computations on the facts.
	 * @throws Exception
	 */
	public void getFactData( ) throws Exception {
		
		this._status = HASIDSConstants.THREAD_ACTIVE;
		
		FileChannel rwChannel = null;
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long position = -1;
		int hashPosition = -1;
		
		if (this._positions == null || this._positions.length <= 0)
			throw new Exception ("No read positions set!");
		
		try {
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception ("File " + this._datasetName + " does not exist!");
			
			long fileSize = (long)_valueLength * (long)_fileRecordCount;
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
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_ONLY, start, mapSize);
				h.put(j, buffer);
				start += mapSize;
			}
			
			if (remaining > 0) {
				MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_ONLY, start, remaining);
				h.put(j, buffer);
			}
			
			System.out.println("No of entries in Hashtable : " + h.size());
			endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("File mapping time : " + diff + "  Milliseconds");
			
            beginTime = System.nanoTime();
			
			MappedByteBuffer buffer = null;
			
			// set the temp buffer to get data
			StringBuffer sb = new StringBuffer(this._valueLength);
			
			// temporary holder of previous hash position for comparison, instead of reading from hashtable.
			int prevHashPosition = -1;
			
			for (int i = 0; i < _positions.length; i++) // 2 millis per 200M
			{
				hashPosition = _positions[i]/mapSize; // 3 millis per 200M
				position = (_positions[i]%mapRecordCount * this._valueLength); // 10 millis per 200M
				
				// create a new String Buffer to hold the raw float and doubles
				sb = new StringBuffer(this._valueLength); // 3400 millis per 200M

				// get the correct buffer for the given position that data must be written
				if (hashPosition != prevHashPosition)
					buffer = h.get(hashPosition); // 4800 millis per 200M
				
				prevHashPosition = hashPosition;
				
				// if we don't find the buffer, break
				// this should never happen, just in case
				if (buffer == null) { // 2 millis per 200M
					System.out.println("Null Buffer");
					this._status = HASIDSConstants.THREAD_FAILED;
					break;
				}
				
				
				//System.out.println("Current position being set = " + (this._positions[i] * this._valueLength)%mapSize);
				// set the buffer position
				buffer.position((int) position); // 750 millis for 200M
				 
				
				// iterate byte by byte and load into temp buffer as characters
				for (int k = 0; k < this._valueLength; k++)
					sb.append((char)buffer.get()); // 2100 millis for 200M/get; 17000 millis for 200M/sb.append
				
				
				// convert the value into either Integer bits or Long bits and then to raw bytes
				// to be written // 7000 millis for 200M
				if (this._dataType == HASIDSConstants.DATA_TYPE_FLOAT) {
					this._values[i] = Float.intBitsToFloat(Integer.parseInt(sb.toString())); 
				}
				else if (this._dataType == HASIDSConstants.DATA_TYPE_DOUBLE) {
					this._values[i] = Double.longBitsToDouble(Long.parseLong(sb.toString()));
				}
				
				// reset the buffer to be garbage collected.
				sb = null;
			}
			
			Enumeration<Integer> e = h.keys();
			while (e.hasMoreElements()) {
				buffer = h.get(e.nextElement());
				buffer.clear();
				buffer = null;
			}
			
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
	            System.out.println("File based fact read time : " + elapsedTimeInMillis + "  Milliseconds");
	            
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
	
	/**
	 * This method is used to get fact data for a given set of ids. These ids must first be
	 * set after the class instance is created using the setDatareadPositions method and are 
	 * stored in an instance variable. The matching facts are stored in an instance variable 
	 * which are retrieved using the getValues method. This method is always run as a single
	 * thread. This method is for getting facts only, the caller assumes the responsibility
	 * of doing any computations on the facts.
	 * @throws Exception
	 */
	public HASIDSFactStructure getFactDataMemory(int[] positions ) throws Exception {
		
		HASIDSFactStructure hfs = new HASIDSFactStructure();
		try {
			float[] memFloatValues = this._hfs.getFloatValues();
			float[] filterFloatValues = new float[positions.length];
			for (int i = 0; i < positions.length; i++)
				filterFloatValues[i] = memFloatValues[positions[i]];
		
			hfs.setFloatValues(filterFloatValues);
		}
		catch (Exception e) {
			;
		}
		
		try {
			double[] memDoubleValues = this._hfs.getDoubleValues();
			double[] filterDoubleValues = new double[positions.length];
			for (int i = 0; i < positions.length; i++)
				filterDoubleValues[i] = memDoubleValues[positions[i]];
		
			hfs.setDoubleValues(filterDoubleValues);
		}
		catch (Exception e) {
			;
		}
		return hfs;
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
			
			//System.out.println((long)Double.parseDouble("1.17112345E18"));
			
			int count = 200;
			
			//System.out.println("Value of 4611686018427387904 = " + Double.longBitsToDouble(Long.parseLong("4611686018427387904")));
			int[] positions = new int[count]; //position
			for (int i = 0; i < count; i++)
				positions[i] = i * 1000000;
				
			double[] values = null;
			TestFactDataReaderByTypes t = new TestFactDataReaderByTypes ("c:\\users\\dpras\\tempdata\\ov2.FC", HASIDSConstants.DATA_TYPE_FLOAT);
			/*t.setDataReadPositions(positions);
			t.getFactData();
			values = t.getValues();
			System.out.println("Number of values received = " + values.length);
			
			for (int j = 0; j < values.length; j++)
				if (j%100000 == 0)
					System.out.println(values[j]);
			*/
			
			
			
			t.loadToMemory();
			HASIDSFactStructure hfs = t.getFactDataMemory(positions);
			float[] floatValues = hfs.getFloatValues();
			System.out.println("Number of values received = " + floatValues.length);
			
			for (int j = 0; j < floatValues.length; j++)
				if (j%(count/10) == 0)
					System.out.println("Value at j = " + j + " = " + floatValues[j]);
			
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
