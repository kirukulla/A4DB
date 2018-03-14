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
import com.hasids.datastructures.CheckSum;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 *
 * This class is a dimension writer component. It allows the creation of data segments 
 * and writing data to the data segments. File sizes are restricted to 2 billion bytes.
 * 
 * For the write to work correctly, the position array must be sorted in ascending order
 * to ensure all writes are complete in a single parse from the lowest position to the
 * highest position
 * 
 * Another approach to conserve memory is to have a write function with the input containing 
 * a byte array of length the is the difference of highest position minus lowest position + 1
 * and then setting the position in the array to the byte value that needs to be written 
 * to the file. The second input would be an integer indicating the position of the file 
 * where the write of the array should begin with. All values in the byte array that designate
 * a write value should have a byte value > 0. 
 * 
 * This is a pure BATCH operation mode to allow massive writes at highest speeds. This class 
 * will be called by the atomizer once it has marked the system as Batch operational mode. 
 * When this mode is set there will a lock on all reads and writes other than the bulk 
 * operation channel.
 */
public class SingleByteDimDataBatchWriter implements Runnable {
	
	/*Writing in raw mode without any synchronization*/

	
	// instance variables associated with the dataset
	private String _dbName;
	private String _datasetName;
	private int _recordCount;
	private RandomAccessFile _randomAccessFile;
	private FileChannel _rwChannel = null;
	private MappedByteBuffer _buffer = null;
	private long _sessionId = 0L;
	private int _lowRange = HASIDSConstants.DIM_MAX_RECORDS + 1;
	private int _highRange = -1;
	
	// current thread write positions and values
	int[] _position; //position
	byte[] _values; // values
	
	public static final short INACTIVE = 0;
	public static final short ACTIVE = 1;
	public static final short COMPLETE = 2;
	public static final short FAILED = 3;
	private int _status = SingleByteDimDataBatchWriter.INACTIVE;
	
	public SingleByteDimDataBatchWriter(String dbName, String datasetName) throws Exception {
		// TODO Auto-generated constructor stub
		this._dbName = dbName;
		this._datasetName = datasetName;
		try {
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			this._recordCount = ((int)f.length()) - CheckSum.FILE_CHECKSUM_LENGTH;
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
			
			CheckSum.validateFile(this._dbName, this._datasetName);
			
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	public SingleByteDimDataBatchWriter(String datasetName, int recordCount) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= 2 billion");
		
		this._recordCount = recordCount;
		
		// generate the session Id for this instance of the write class
		this._sessionId = System.nanoTime();
				
		try {
			this.createSegment();
		}
		catch (Exception e) {
			// commented out for now
			//throw new Exception(e.getMessage());
		}
	}
	
	public String getDatasetName() {
		return this._datasetName;
	}
	
	public int getRecordCount() {
		return this._recordCount;
	}
	
	public long getSessionId() {
		return this._sessionId;
	}
	
	/**
	 * Method that stores the data positions (record ids) and values to be written to the
	 * file. This is a synchronized method. Record ids that are being written are locked
	 * first, retry table entries populated if ids are locked by a different session.
	 * 
	 * @param position An array of ids whose values must be set
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	
	public synchronized void setWriteDataPositionBuffer(int[] position, byte[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		
		// Determine the low and high ranges so that the precise mapping range can be established
		boolean writeStatus = true;
		String message = null;
		int i = -1;
		for (i = 0; i < position.length; i++)
		{
			if (position[i] < 0 || position[i] > this._recordCount - 1) {
				writeStatus = false;
				message = "Positions to be set should be >= 0 and <= file length/record count";
				break;
			}
			
			if (values[i] < 0 || values[i] > 255) {
				writeStatus = false;
				message = "Values to be set should be >= 0 and <= 255; 0 is reserved to mean no change";
				break;
			}
			
			if (this._lowRange > position[i])
				this._lowRange = position[i];
			
			if (this._highRange < position[i])
				this._highRange = position[i];
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		this._values = values;
		this._position = position;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	/**
	 * Create a segment with the record size. Each byte in the segment is set to null value
	 * to begin with. Null is ASCII code zero. This is a synchronized method. It ensures that 
	 * no two threads create the same segment
	 * 
	 * @throws Exception
	 */
	private synchronized void createSegment() throws Exception {
		this._status = SingleByteDimDataBatchWriter.ACTIVE;
		//byte buf = 0;
		
		long beginTime = System.nanoTime();
		
		try {
			File f = new File(this._datasetName);
			if (f.exists())
				throw new Exception("File " + this._datasetName + " exists! Cannot create segment");
			
			RandomAccessFile randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			FileChannel rwChannel = randomAccessFile.getChannel();
			MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, this._recordCount + CheckSum.FILE_CHECKSUM_LENGTH);
			
			/*buffer.position(CheckSum.FILE_CHECKSUM_LENGTH);
			for (int i = CheckSum.FILE_CHECKSUM_LENGTH; i < this._recordCount + CheckSum.FILE_CHECKSUM_LENGTH; i++)
			{
				buffer.put(buf);
			}*/
			
			// Write the CL at the beginning of the file
			buffer.position(0); // reset to position 0
			buffer.put((byte)1); // write the file type - DIM 1
			buffer.put((byte)1); // write the encoding type - US ASCII single byte
			buffer.put((byte)0); // write the data length byte 0 as 0
			buffer.put((byte)0); // write the data length byte 1 as 0
			buffer.put((byte)0); // write the data length byte 2 as 0
			buffer.put((byte)1); // write the data length byte 3 as 1
			buffer.put((byte)0); // write the decimal length byte 0 as 0
			buffer.put((byte)0); // write the decimal length byte 1 as 0
			
			// write the dataset name and data set size
			buffer.put(CheckSum.computeCS(this._dbName + "|" + this._datasetName, (this._recordCount + CheckSum.FILE_CHECKSUM_LENGTH)));
			
			// calculate the current time
			long lastModifiedTime = System.currentTimeMillis();
			
			// Add it to the buffer
			buffer.put(Long.toString(CheckSum.computeTFS(lastModifiedTime)).getBytes());
			buffer.force();
			rwChannel.close();
			randomAccessFile.close();
			
			// set the last modified time
			f.setLastModified(lastModifiedTime);
			
			System.out.println("File " + this._datasetName + " length = " + f.length());
			
		}
		catch(Exception e) {
			this._status = SingleByteDimDataBatchWriter.FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == SingleByteDimDataBatchWriter.ACTIVE)
				this._status = SingleByteDimDataBatchWriter.COMPLETE;
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("Segment creation time for : " + this._recordCount + " = " + elapsedTimeInMillis + "  Milliseconds");
            
			try {
				if (_rwChannel != null)
					_rwChannel.close();
				if (_randomAccessFile != null)
					_randomAccessFile.close();
			}
			catch (Exception e) {
				;
				//throw new Exception(e.getMessage());
			}
		}
	}
	
	/**
	 * Method to write the data set using the setter method to the dataset
	 * 
	 * @throws Exception
	 */
	public void writeToSegment() throws Exception {
		
		this._status = SingleByteDimDataBatchWriter.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			//if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
			//	throw new Exception("Null Data or data length does not match values length! No data to write");
			
			File f = new File(this._datasetName);
			if (!f.exists())
				this.createSegment();
			
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			this._rwChannel = _randomAccessFile.getChannel();
			
			
			this._buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, (this._lowRange + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1));
			
			// iterate through the positions and values and write to file
			for (int i = 0; i < _position.length; i++)
				this._buffer.put((this._position[i] - this._lowRange), this._values[i]);
			
			// commit
			this.commit();
			
		}
		catch(Exception e) {
			this._status = SingleByteDimDataBatchWriter.FAILED;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == SingleByteDimDataBatchWriter.ACTIVE)
				this._status = SingleByteDimDataBatchWriter.COMPLETE;
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("File based read/write time : " + elapsedTimeInMillis + "  Milliseconds");
			
		}
	}
	
	/***
	 * Method to commit the changes associated with this data writer
	 * 
	 * @throws Exception
	 */
	private synchronized void commit() throws Exception {
		
		// Write the TS
		long lastModifiedTime = this.writeCSTS();
					
					
		_buffer.force();
		_rwChannel.close();
		_randomAccessFile.close();
		
		_buffer = null;
		_rwChannel = null;
		_randomAccessFile = null;
		
		// set the last modified time
		File f = new File(this._datasetName);
		f.setLastModified(lastModifiedTime);
	}
	
	/**
	 * Method to write the timestamp to the checksum field
	 */
	private long writeCSTS() {
		long retVal = System.currentTimeMillis();
		try {
			// At the end map the TS portion of the CS
			MappedByteBuffer buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, CheckSum.FILE_DATASET_TIME_POS, CheckSum.FILE_DATASET_TIME_LEN);
			buffer.put(Long.toString(CheckSum.computeTFS(retVal)).getBytes());
			buffer.force();
			buffer = null;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return retVal;
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
		
		int fileSize = 10;
		String datasetName = "c:\\\\users\\\\dpras\\\\tempdata\\\\testCS_2.DM";
		try {
			SingleByteDimDataBatchWriter t = new SingleByteDimDataBatchWriter (datasetName, fileSize);
			
			
			/*long beginTime = System.nanoTime();
			int[] data = new int[fileSize]; //position
			byte[] values = new byte[fileSize]; // values
			
			for (int i = 0; i < fileSize; i++) {
				data[i] = i;
				values[i] = 67;
			}
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("Data preperation time : " + elapsedTimeInMillis + "  Milliseconds");
			
			t.setWriteDataPositionBuffer(data, values, false, false);
			t.writeToSegment();*/
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
