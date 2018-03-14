/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.dim;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DimDataWriter implements Runnable {
	
	/*lock - begin transaction, update/write, retry success, update/write, unlock - end transaction, commit*/
	/*lock - begin transaction, update/write, retry failure, rollback - update/write original, unlock - end transaction*/
	
	// Data write buffer that is static
	private static Hashtable<String, Hashtable<Integer, int[]>> LOCK_TABLE = 
			new Hashtable<String, Hashtable<Integer, int[]>>();
	
	public Logger logger = LoggerFactory.getLogger(DimDataWriter.class);
	
	// The position table associated with a dataset stored within the LOCK_TABLE
	Hashtable<Integer, int[]> _positionTable = null;
	
	// The position and values table for setting data values
	Hashtable<Integer, Integer> _posValuesTable = null;
	
	// instance variables determining whether partial updates are allowed
	// if partial updates are not allowed, then an exception will be thrown back
	// in case any of the records are locked by another session
	// if partial updates are allowed then those ids that are unlocked are updated
	private boolean _allowPartial = false;
	
	// retry flag, this flag indicates if those records not available due to lock
	// by a different instance must be retried for updates after the rest are complete
	private Hashtable<Integer, Integer> _retryTable = new Hashtable<Integer, Integer>();
	
	// retry flag that determines if records that could not be written must be retried
	// for updates.
	private boolean _retryFlag = false; 
	
	/*Writing in raw mode without any synchronization*/
	private int _operationalMode = HASIDSConstants.OPERATION_MODE_ONLINE;
	
	// instance variables associated with the dataset
	private String _dbName;
	private String _datasetName;
	private int _recordCount;
	
	// Check sum values
	private int _fileType;
	private int _encoding;
	private int _segmentNo;
	private int _dataLength;
	private short _decimals = 0;
	
	private RandomAccessFile _randomAccessFile;
	private FileChannel _rwChannel = null;
	private MappedByteBuffer _buffer = null;
	private long _sessionId = 0L;
	private int _lowRange = HASIDSConstants.DIM_MAX_RECORDS + 1;
	private int _highRange = -1;
	
	// current thread write positions and values
	int[] _position; //position
	int[] _values; // values
	int _positionsLength = 0;
	
	public static final short INACTIVE = 0;
	public static final short ACTIVE = 1;
	public static final short COMPLETE = 2;
	public static final short FAILED = 3;
	private int _status = DimDataWriter.INACTIVE;
	
	// Hashtable to store the value of each domain being written along with the low, high and count
	Hashtable<Integer, int[]> collectionTable = new Hashtable<Integer, int[]>(); 
	
	public DimDataWriter(String dbName, String datasetName, int operationMode) throws Exception {
		
		this.setFileType();
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._datasetName = datasetName;
		try {
			if (operationMode != HASIDSConstants.OPERATION_MODE_BATCH &&
					operationMode != HASIDSConstants.OPERATION_MODE_ONLINE)
				throw new Exception ("Operation mode invalid, must be either batch or online");
			
			this._operationalMode = operationMode;
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			
			int[] fileType = new int[1];
			int[] encoding = new int[1];
			int[] segmentNo = new int[1];
			int[] datasize = new int[1];
			short[] decimals = new short[1];
			
			CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			this._recordCount = (((int)f.length()) - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
			logger.info("Record count from CheckSum: " + this._recordCount);
			
			/*if (fileType[0] != CheckSum.FILE_TYPE_DIM)
				throw new Exception("Invalid file type!");
			
			if (encoding[0] < CheckSum.DIM_ENCODE_TYPE1 || encoding[0] > CheckSum.DIM_ENCODE_TYPE3)
				throw new Exception ("Invalid encoding type in header, Dimension datasets data length must be >= " + 
						CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
			
			if ((encoding[0] == CheckSum.DIM_ENCODE_TYPE1 && datasize[0] != 1) ||
					(encoding[0] == CheckSum.DIM_ENCODE_TYPE2 && datasize[0] != 2) ||
					(encoding[0] == CheckSum.DIM_ENCODE_TYPE3 && datasize[0] != 4))
				throw new Exception ("Check Sum error, encoding and size do not match");
			
			
			this._dataLength = datasize[0];
			this._encoding = encoding[0];
			*/
			
			this.checkEncodingExisting(fileType[0], encoding[0], datasize[0], decimals[0]);
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
			
			
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	public DimDataWriter(String dbName, String datasetName, int recordCount, int encoding, int dataLength, short decimals, int segmentNo) throws Exception {
		
		// when opening in write mode for the first time, default it to batch mode
		this._operationalMode = HASIDSConstants.OPERATION_MODE_BATCH;
		
		// set the file type
		this.setFileType();
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		
		/*
		if (encoding < CheckSum.DIM_ENCODE_TYPE1 || encoding > CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Dimension datasets data encoding type must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		if (encoding == CheckSum.DIM_ENCODE_TYPE1 || encoding == CheckSum.DIM_ENCODE_TYPE2)
			this._dataLength = encoding;
		else if (encoding == CheckSum.DIM_ENCODE_TYPE3)
			this._dataLength = 4; // 4 bytes
		
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS/this._dataLength) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= " + (HASIDSConstants.DIM_MAX_RECORDS/this._dataLength));
		
		this._encoding = encoding;
		
		this._recordCount = recordCount;
		*/
		
		this.checkEncodingNew(encoding, recordCount, dataLength, decimals, segmentNo);
		
		// generate the session Id for this instance of the write class
		this._sessionId = System.nanoTime();
				
		try {
			this.createSegment();
		}
		catch (Exception e) {
			// commented out for now
			throw new Exception(e.getMessage());
		}
	}
	
	protected void setFileType() {
		this._fileType = CheckSum.FILE_TYPE_DIM;
	}
	
	protected void checkEncodingNew(int encoding, int recordCount, int dataLength, short decimals, int segmentNo) throws Exception {
		if (encoding < CheckSum.DIM_ENCODE_TYPE1 || encoding > CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Dimension datasets data encoding type must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		if (encoding == CheckSum.DIM_ENCODE_TYPE1 || encoding == CheckSum.DIM_ENCODE_TYPE2)
			this._dataLength = encoding;
		else if (encoding == CheckSum.DIM_ENCODE_TYPE3)
			this._dataLength = 4; // 4 bytes
		
		if (segmentNo < 0)
			throw new Exception ("Segment number cannot be < 0!");
		
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS/this._dataLength) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= " + (HASIDSConstants.DIM_MAX_RECORDS/this._dataLength));
		
		this._encoding = encoding;
		
		this._recordCount = recordCount;
		
		this._segmentNo = segmentNo;
	}
	
	protected void checkEncodingExisting(int fileType, int encoding, int datasize, short decimals) throws Exception {
		if (fileType != CheckSum.FILE_TYPE_DIM)
			throw new Exception("Invalid file type!");
		
		if (encoding < CheckSum.DIM_ENCODE_TYPE1 || encoding > CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Invalid encoding type in header, Dimension datasets data length must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		if ((encoding == CheckSum.DIM_ENCODE_TYPE1 && datasize != 1) ||
				(encoding == CheckSum.DIM_ENCODE_TYPE2 && datasize != 2) ||
				(encoding == CheckSum.DIM_ENCODE_TYPE3 && datasize != 4))
			throw new Exception ("Check Sum error, encoding and size do not match");
		
		this._dataLength = datasize;
		this._encoding = encoding;
	}

	
	/**
	 * Create a segment with the record size. Each byte in the segment is set to null value
	 * to begin with. Null is ASCII code zero. This is a synchronized method. It ensures that 
	 * no two threads create the same segment
	 * 
	 * @throws Exception
	 */
	private void createSegment() throws Exception {
		this._status = DimDataWriter.ACTIVE;
		//byte buf = 0;
		
		long beginTime = System.nanoTime();
		
		try {
			File f = new File(this._datasetName);
			if (f.exists())
				throw new Exception("File " + this._datasetName + " exists! Cannot create segment");
			
			RandomAccessFile randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			FileChannel rwChannel = randomAccessFile.getChannel();
			MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, (this._recordCount * this._dataLength) + CheckSum.FILE_CHECKSUM_LENGTH);
			
			// Write the CL at the beginning of the file
			buffer.position(0); // reset to position 0
			buffer.put(this.getFileType()); // write the file type - DIM 1
			buffer.put(this.getEncoding()); // write the encoding type
			buffer.putInt(this.getSegmentNo()); // write the segment no
			
			//buffer.put((byte)0); // write the data length byte 0 as 0
			//buffer.put((byte)0); // write the data length byte 1 as 0
			//buffer.put((byte)0); // write the data length byte 2 as 0
			//buffer.put((byte)this._dataLength); // write the data length byte 3 as 1
			buffer.putInt(this.getDataLength());
			
			//buffer.put((byte)0); // write the decimal length byte 0 as 0
			//buffer.put((byte)0); // write the decimal length byte 1 as 0
			buffer.putShort(this.getDecimals());
			
			// write the dataset name and data set size
			buffer.put(CheckSum.computeCS(this.getDbName() + "|" + this.getDatasetName(), ((this.getRecordCount() * this.getDataLength()) + CheckSum.FILE_CHECKSUM_LENGTH)));
			
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
			this._status = DimDataWriter.FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == DimDataWriter.ACTIVE)
				this._status = DimDataWriter.COMPLETE;
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
	
	
	
	/*Getters*/
	
	public int getOperationalMode() {
		return _operationalMode;
	}

	public String getDbName() {
		return _dbName;
	}

	public int getDataLength() {
		return _dataLength;
	}

	public byte getEncoding() {
		return (byte)_encoding;
	}

	public int getLowRange() {
		return _lowRange;
	}

	public int getHighRange() {
		return _highRange;
	}

	public int getStatus() {
		return _status;
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

	public byte getFileType() {
		return (byte)_fileType;
	}

	public short getDecimals() {
		return _decimals;
	}
	
	public int getSegmentNo() {
		return _segmentNo;
	}
	
	/**
	 * This method returns a Hashtable whose key is the domain value and whose value is a 3 
	 * dimension array. The first dimension stores the low range for the key, second dimension
	 * stores the high range for the key and the third dimension stores the count for the key.
	 * @return
	 */
	public Hashtable<Integer, int[]> getCollectionTable() {
		return this.collectionTable;
	}
	
	private boolean checkPositionsValues(int[] position, int[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		int minValue = -1;
		int maxValue = -1;
		
		if (this._dataLength == 1) {
			minValue = HASIDSConstants.DIM_ENCODE_TYPE1_MIN;
			maxValue = HASIDSConstants.DIM_ENCODE_TYPE1_MAX;
		}
		else if (this._dataLength == 2) {
			minValue = HASIDSConstants.DIM_ENCODE_TYPE2_MIN;
			maxValue = HASIDSConstants.DIM_ENCODE_TYPE2_MAX;
		}
		else if (this._dataLength == 4) {
			minValue = HASIDSConstants.DIM_ENCODE_TYPE3_MIN;
			maxValue = HASIDSConstants.DIM_ENCODE_TYPE3_MAX;
		}
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		int i = -1;
		int[] collectionValue = null;
		for (i = 0; i < position.length; i++)
		{
			if (position[i] < 0 || position[i] > this._recordCount - 1) {
				writeStatus = false;
				message = "Positions to be set should be >= 0 and <= file length/record count";
				break;
			}
			
			if (values[i] < minValue || values[i] > maxValue) {
				writeStatus = false;
				message = "Values to be set should be >= " + minValue + " and <= " + maxValue + "; 0 is reserved to mean no change";
				break;
			}
			
			if (this._lowRange > position[i])
				this._lowRange = position[i];
			
			if (this._highRange < position[i])
				this._highRange = position[i];
			
			// Add to collection Table
			collectionValue = collectionTable.get(values[i]);
			if (collectionValue == null) {
				// create and initialize the collection value
				collectionValue = new int[] {2000000001,-1,0};
				collectionTable.put(values[i], collectionValue);
			}
			
			++collectionValue[2];
			
			if (position[i] < collectionValue[0])
				collectionValue[0] = position[i];
			
			if (position[i] > collectionValue[1])
				collectionValue[1] = position[i];
			
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}

	/**
	 * Method that stores the data positions (record ids) and values to be written to the
	 * file. 
	 * 
	 * @param position An array of ids whose values must be set
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	private void setWriteDataPositionBufferBatch(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._values = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}

	/**
	 * Method that stores the data positions (record ids) and values to be written to the
	 * file.  
	 * 
	 * @param position An array of ids whose values must be set and are in ascending order
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	private Hashtable<Integer, Integer> setWriteDataPositionBufferOnline(int[] positions, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Integer> sReturn = new Hashtable<Integer, Integer>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(positions, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = DimDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, int[]>(positions.length);
				DimDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
			}
		}
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate position table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus) {
			throw new Exception (message);
		}
		
		// create the position values hashtable
		this._posValuesTable = new Hashtable<Integer, Integer>(positions.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
		// set the byte order
		this._buffer.order(ByteOrder.LITTLE_ENDIAN);
		
		System.out.println("Length of Buffer : " + this._buffer.limit());
		this._buffer.clear();
		
		/************************************************************************************************
		 * ADD CODE TO MAP ORIGINAL BUFFER TO A TEMP BUFFER FOR ROLL BACK IN BATCH MODE
		 ************************************************************************************************
		 **/
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to Map file into memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
        synchronized (this._positionTable) {
	        for (i = 0; i < positions.length; i++) {
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(positions[i])) {
					
					// lock the position by adding it to the position table
					if (this._dataLength == 1)
						this._positionTable.put(positions[i], new int[] {values[i], this._buffer.get((positions[i] - this._lowRange))});
					else if(this._dataLength == 2)
						this._positionTable.put(positions[i], new int[] {values[i], this._buffer.getShort((positions[i] - this._lowRange) * this._dataLength)});
					else if(this._dataLength == 4)
						this._positionTable.put(positions[i], new int[] {values[i], this._buffer.getInt((positions[i] - this._lowRange) * this._dataLength)});
					
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(positions[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(positions[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag)
						this._retryTable.put(positions[i], values[i]); 
				}			
			}
        }
        
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		// close the buffers for the write operation to open again
		this._buffer = null;
		this._rwChannel.close();
		this._randomAccessFile.close();
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus) {
			throw new Exception (message);
		}
		
		return sReturn;
		
	}
	
	/**
	 * Method to remove the keys associated with the current session from the position table
	 */
	private void removeKeysFromTable() {
		
		long beginTime = System.nanoTime();
		
		// do we have a valid position values table
		if (this._posValuesTable == null || this._posValuesTable.size() <= 0) return;
		
		// do we have a valid position table
		if (this._positionTable == null)
			this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
		
		// remove from the position table all the keys associated with pos values buffer
		Iterator<Integer> it = this._posValuesTable.keySet().iterator();
		Integer i = null;
		while(it.hasNext()) {
			i = it.next();
			this._positionTable.remove(i);
		}
		
		// if position table is empty, remove it from the lock table
		if (this._positionTable.size() <= 0)
			LOCK_TABLE.remove(this._dbName + "|" + this._datasetName);
		
		// clear the pos values table
		this._posValuesTable.clear();
		
		// remove from retry table
		if (this._retryTable != null && this._retryTable.size() > 0)
			this._retryTable.clear();
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to clear keys = " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	

	
	/**
	 * Method to write single bytes into the buffer
	 * 
	 * @throws Exception
	 */
	private void writeToSegmentTypeByte() throws Exception {
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++)
				this._buffer.put((this._position[i] - this._lowRange), (byte)this._values[i]);
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			while (e.hasMoreElements()) {
				i = e.nextElement();
				
				// write to dataset
				this._buffer.put((i - this._lowRange), (byte)this._positionTable.get(i)[0]);
				
			}
		}
	}

	/**
	 * Method to write single bytes into the buffer
	 * 
	 * @throws Exception
	 */
	private void writeToSegmentTypeShort() throws Exception {
		//byte[] shortBytes = new byte[2];
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//shortBytes = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short)this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(shortBytes);
				this._buffer.putShort((short)this._values[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			int value = 0;
			Enumeration<Integer> e = this._posValuesTable.keys();
			while (e.hasMoreElements()) {
				i = e.nextElement();
				
				value = this._posValuesTable.get(i);
				// write to dataset
				this._buffer.putShort((i - this._lowRange) * this._dataLength, (short)value);
				
			}
		}
	}
	
	/**
	 * Method to write single bytes into the buffer
	 * 
	 * @throws Exception
	 */
	private void writeToSegmentTypeInt() throws Exception {
		//byte[] intBytes = new byte[4];
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//intBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(intBytes);
				this._buffer.putInt(this._values[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			while (e.hasMoreElements()) {
				i = e.nextElement();
				
				// write to dataset
				this._buffer.putInt((i - this._lowRange) * this._dataLength, this._positionTable.get(i)[0]);
				
			}
		}
	}
	
	/**
	 * Method to retry writing values for positions in the retry table.
	 */
	private void retry() throws Exception {
		if (this._retryTable == null || this._retryTable.size() <= 0) return;
		
		if (this._positionTable == null)
			this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
		
		
		// we will keep retrying till the timeout is reached
		int iterations = HASIDSConstants.RETRY_LIMIT_MILLIS/HASIDSConstants.RETRY_INCREMENT_MILLIS;
		
		// loop through the number of iterations
		for (int j = 0; j < iterations; j++) {
			
			Integer i = null;
			
			// get the list of positions in the retry table
			Enumeration<Integer> e = this._retryTable.keys();
			if (this._dataLength == 1) {
				int b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new int[] {b, this._buffer.get((i - this._lowRange))});
					
						// write to the position in the memory buffer
						this._buffer.put((i - this._lowRange), (byte)b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == 2) {
				int b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new int[] {b, this._buffer.getShort((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putShort((i - this._lowRange) * this._dataLength, (short)b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == 4) {
				int b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new int[] {b, this._buffer.getInt((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putInt((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			
			if (this._retryTable.size() <= 0)
				break;
			
			// sleep to enable other threads to complete and free up the locks
			Thread.sleep(HASIDSConstants.RETRY_INCREMENT_MILLIS);
		}
		
		// if after the session time out is reached and we still have locked positions
		// as in the retry table
		if (this._retryTable.size() > 0)
			throw new Exception ("Session timeout!");
		
	}
	
	/***
	 * Method to rollback the changes associated with the data writer
	 * 
	 * @throws Exception
	 */
	public void rollback() throws Exception {
		System.out.println("Rollback, posValuesTable size : " + this._posValuesTable.size());
		if (this._posValuesTable != null && this._posValuesTable.size() > 0) {
			// rewrite the old data back
			Iterator<Integer> it = this._posValuesTable.keySet().iterator();
			
			if (this._dataLength == 1) {
				int i = -1;
				int b;
		
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
		
					// get the original byte value
					b = this._positionTable.get(i)[1];
			
					// if the byte value is zero, it means that this position was never written in the file
					this._buffer.put((i - this._lowRange), (byte)b);
				}
			}
			else if (this._dataLength == 2) {
				int i = -1;
				int b;
		
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
		
					// get the original byte value
					b = this._positionTable.get(i)[1];
			
					// if the byte value is zero, it means that this position was never written in the file
					this._buffer.putShort((i - this._lowRange) * this._dataLength, (short)b);
				}
			}
			else if (this._dataLength == 4) {
				int i = -1;
				int b;
		
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
		
					// get the original byte value
					b = this._positionTable.get(i)[1];
			
					// if the byte value is zero, it means that this position was never written in the file
					this._buffer.putInt((i - this._lowRange) * this._dataLength, b);
				}
			}
		}
		
		// commit
		this.commit();
	}	
	
	/***
	 * Method to commit the changes associated with this data writer
	 * 
	 * @throws Exception
	 */
	public void commit() throws Exception {
		
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
		
		// remove the keys
		this.removeKeysFromTable();
	}
	
	/**
	 * Method to write the timestamp to the checksum field
	 */
	private long writeCSTS() {
		long retVal = System.currentTimeMillis();
		try {
			// At the end map the TS portion of the CS
			if (this._rwChannel != null) {
				MappedByteBuffer buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, CheckSum.FILE_DATASET_TIME_POS, CheckSum.FILE_DATASET_TIME_LEN);
				buffer.put(Long.toString(CheckSum.computeTFS(retVal)).getBytes());
				buffer.force();
				buffer = null;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return retVal;
	}
	


	/**
	 * Method that stores the data positions (record ids) and values to be written to the
	 * file. This is a synchronized method. Record ids that are being written are locked
	 * first, retry table entries populated if ids are locked by a different session. 
	 * 
	 * @param position An array of ids whose values must be set and are in ascending order
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	public Hashtable<Integer, Integer> setWriteDataPositionBuffer(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		Hashtable<Integer, Integer> retValue = new Hashtable<Integer, Integer>(1);
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	/**
	 * Method to delete record ids from the dataset
	 * 
	 * @param position
	 * @param allowPartial
	 * @param retryFlag
	 * @return
	 * @throws Exception
	 */
	public Hashtable<Integer, Integer> deleteRecords(int[] position, boolean allowPartial, boolean retryFlag) throws Exception {
		Hashtable<Integer, Integer> retValue = new Hashtable<Integer, Integer>(1);
		if (position == null || position.length == 0)
			throw new Exception("Invalid records ids to delete! Use truncate for deleting all records!!");
		
		int count = position.length;
		int[] values = new int[count];
		for (int i = 0; i < count; i++)
			values[i] = 0;
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	/***
	 * Method that returns the locked record identifiers and their original values in a Hashtable.
	 * 
	 * @param datasetName Dataset name for which record identifiers and original values in locked status are required
	 * @return Hashtable with keys of Integers and values of Bytes.
	 */
	public static Hashtable<Integer, Integer> getLockedKeys(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Integer> returnTable = new Hashtable<Integer, Integer>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, int[]> positionTable = DimDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			int b = 0;
			
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				b = positionTable.get(i)[1];
				if (b != 0) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	/**
	 * Method to write the data set using the setter method to the dataset
	 * 
	 * @throws Exception
	 */
	public void writeToSegment(boolean autoCommit) throws Exception {
		
		this._status = DimDataWriter.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			//if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
			//	throw new Exception("Null Data or data length does not match values length! No data to write");
			
			File f = new File(this._datasetName);
			if (!f.exists())
				this.createSegment();
			
			// create random file
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			// get file channel
			this._rwChannel = _randomAccessFile.getChannel();
			// map file to memory
			this._buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, ((this._lowRange * this._dataLength) + CheckSum.FILE_CHECKSUM_LENGTH), ((this._highRange - this._lowRange + 1) * this._dataLength));
			// set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            this._buffer.order(ByteOrder.LITTLE_ENDIAN);
            
			// based on the data length we have to write the bytes
			if (this._dataLength == CheckSum.DIM_ENCODE_TYPE1)
				this.writeToSegmentTypeByte();
			else if (this._dataLength == CheckSum.DIM_ENCODE_TYPE2)
				this.writeToSegmentTypeShort();
			else if (this._dataLength == CheckSum.DIM_ENCODE_TYPE3 + 1)
				this.writeToSegmentTypeInt();
			
			// check for the retry table.
			if (this._retryTable.size() > 0) {
				this.retry();
			}
									
			// if we are here and if the autocommit flag is set, we can commit
			if (autoCommit)
				this.commit();
			
		}
		catch(Exception e) {
			this._status = DimDataWriter.FAILED;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == DimDataWriter.ACTIVE)
				this._status = DimDataWriter.COMPLETE;
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("File based read/write time : " + elapsedTimeInMillis + "  Milliseconds");
			
		}
	}	
	
	public void run() {
		// when operating in a thread mode, always set auto commit to false
		// The initiator of the thread must call the commit
		try {
			this.writeToSegment(false);
		}
		catch (Exception e) {
			;// log this
		}
	}

	/*public static void main(String[] args) {
		
		
		int fileSize = 1000000000;
		String dbName = "Test";
		String datasetName1 = "c:\\users\\dpras\\tempdata\\testCS1_1.DM";
		String datasetName2 = "c:\\users\\dpras\\tempdata\\testCS2_1.DM";
		String datasetName3 = "c:\\users\\dpras\\tempdata\\testCS3_1.DM";
		try {
			
			//DimDataWriter t1 = new DimDataWriter (dbName, datasetName1, fileSize, 1, 0, (short)0);
			int[] positions = new int[fileSize];
			int[] values = new int[fileSize];
			
			for (int i = 0; i < fileSize; i++) {
				positions[i] = i;
			}
			
			long beginTime = System.nanoTime();
			for (int i = 0; i < positions.length; i++) {
				values[i] = 0;
			}
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 1 : " + diff);
			
			beginTime = System.nanoTime();
			for (int i = 0; i < fileSize; i++) {
				values[i] = 0;
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for Array initialization 2 : " + diff);
			
			//t1.setWriteDataPositionBuffer(positions, values, false, false);
			//t1.writeToSegment();
			
			
			
			DimDataWriter t1 = new DimDataWriter (dbName, datasetName1, fileSize, 1);
			DimDataWriter t2 = new DimDataWriter (dbName, datasetName2, fileSize, 2);
			DimDataWriter t3 = new DimDataWriter (dbName, datasetName3, fileSize, 3);
			
			//DimDataWriter t1 = new DimDataWriter (dbName, datasetName1, HASIDSConstants.OPERATION_MODE_BATCH);
			//DimDataWriter t2 = new DimDataWriter (dbName, datasetName2, HASIDSConstants.OPERATION_MODE_BATCH);
			//DimDataWriter t3 = new DimDataWriter (dbName, datasetName3, HASIDSConstants.OPERATION_MODE_BATCH);
			
			int[] positions = new int[fileSize];
			int[] values = new int[fileSize];
			
			for (int i = 0; i < fileSize; i++) {
				positions[i] = i;
				values[i] = i - 128;
			}
			
			t1.setWriteDataPositionBuffer(positions, values, false, false);
			t1.writeToSegment();
			
			t2.setWriteDataPositionBuffer(positions, values, false, false);
			t2.writeToSegment();
			
			t3.setWriteDataPositionBuffer(positions, values, false, false);
			t3.writeToSegment();
			
			
			
			short shortValueMax = Short.MAX_VALUE;
			long longValueMax = Long.MAX_VALUE;
			int intValueMax = Integer.MAX_VALUE;
			float floatValueMax = Float.MAX_VALUE;
			double doubleValueMax = Double.MAX_VALUE;
			
			short shortValueMin = Short.MIN_VALUE;
			long longValueMin = Long.MIN_VALUE;
			int intValueMin = Integer.MIN_VALUE;
			float floatValueMin = Float.MIN_VALUE;
			double doubleValueMin = Double.MIN_VALUE;
			
			byte[] shortbytes = new byte[2];
			byte[] longDoublebytes = new byte[8];
			byte[] intFloatbytes = new byte[4];
			
			short retValueShort = 0;
			int retValueInt = 0;
			long retValueLong = 0;
			float retValueFloat = 0;
			double retValueDouble = 0;
			
			// short
			long beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				shortbytes = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(shortValueMin).array();
			}
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of short min 1 billion times : " + diff);
			
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				shortbytes = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(shortValueMax).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of short max 1 billion times : " + diff);

			// get return values
			// get short
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				retValueShort = ByteBuffer.wrap(shortbytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer to short conversion 1 billion times (" + retValueShort + ") : " + diff);

			// integer
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				intFloatbytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(intValueMin).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of int min 1 billion times : " + diff);
			
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				intFloatbytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(intValueMax).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of int max 1 billion times : " + diff);

			// get return values
			// get int
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				retValueInt = ByteBuffer.wrap(intFloatbytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer to integer conversion 1 billion times (" + retValueInt + ") : " + diff);

			// float
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				intFloatbytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(floatValueMin).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of float min 1 billion times : " + diff);
			
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				intFloatbytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(floatValueMax).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of float max 1 billion times : " + diff);

			// get return values
			// get Float
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				retValueFloat = ByteBuffer.wrap(intFloatbytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer to float conversion 1 billion times (" + retValueFloat + ") : " + diff);

			// long
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				longDoublebytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(longValueMin).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of long min 1 billion times : " + diff);
			
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				longDoublebytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(longValueMax).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of long max 1 billion times : " + diff);

			// get return values
			// get long
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				retValueLong = ByteBuffer.wrap(longDoublebytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer to long conversion 1 billion times (" + retValueLong + ") : " + diff);

			// double
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				longDoublebytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(doubleValueMin).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of double min 1 billion times : " + diff);
			
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				longDoublebytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(doubleValueMax).array();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer conversion of double max 1 billion times : " + diff);

			// get return values
			// get double
			beginTime = System.nanoTime();
			for (int count = 0; count < 1000000000; count++) {
				retValueDouble = ByteBuffer.wrap(longDoublebytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
			}
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time for byte buffer to double conversion 1 billion times (" + retValueDouble + ") : " + diff);
			

			


			
			
			
			
			
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}*/

}
