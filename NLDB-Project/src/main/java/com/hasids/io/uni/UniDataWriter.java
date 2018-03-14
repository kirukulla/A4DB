/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.uni;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 *
 * This class is a unary writer component. It allows the creation of data segments 
 * and writing data to the data segments. File sizes are restricted to 2 billion bytes.
 * 
 * For optimized reads the data must be first sorted and written. The data post checksum
 * is a series of data identifiers or record identifiers.
 * 
 * There is a pure BATCH operation mode to allow massive writes at highest speeds. Unary 
 * datasets are pure analytical structures.
 */
public class UniDataWriter implements Runnable {
	
	private static Hashtable<String, Hashtable<MappedByteBuffer, long[]>> writers =
			new Hashtable<String, Hashtable<MappedByteBuffer, long[]>>();
	
	// instance variables associated with the dataset
	private String _dbName;
	private String _datasetName;
	
	// check sum variables
	private int _fileType = CheckSum.FILE_TYPE_UNI;
	private int _segmentCount;
	private int _currentRecordCount;
	private int _dataLength;
	private short _decimals;
	private int _messageTypeSize;
	private short _messageType;
	private int _encoding;
	private int _segmentNo;
	
	private int _startPoint;
	private int _endPoint;
	
	private RandomAccessFile _randomAccessFile;
	private FileChannel _rwChannel = null;
	private MappedByteBuffer _buffer = null;
	private ByteBuffer _tempBuffer = null;
	private long _sessionId = 0L;
	
	// current thread write positions and values
	private int[] _recordIds; // values
	private short[] _counts;
	
	public static final short INACTIVE = 0;
	public static final short ACTIVE = 1;
	public static final short COMPLETE = 2;
	public static final short FAILED = 3;
	private int _status = UniDataWriter.INACTIVE;
	
	/**
	 * Constructor when appending to an existing dataset.
	 * 
	 * @param dbName database name
	 * @param datasetName dataset name
	 * @throws Exception
	 */
	public UniDataWriter(String dbName, String datasetName, Short messageType) throws Exception {
		// TODO Auto-generated constructor stub
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._datasetName = datasetName;
		
		try {
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			
			int[] fileType = new int[1];
			int[] encoding = new int[1];
			int[] datasize = new int[1];
			short[] decimals = new short[1];
			int[] segmentCount = new int[1];
			int[] segmentNo = new int[1];
			
			CheckSum.validateUNIFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo, segmentCount);
			
			this._segmentNo = segmentNo[0];
			
			if (fileType[0] != CheckSum.FILE_TYPE_UNI)
				throw new Exception("Invalid file type!");
			
			if (encoding[0] != CheckSum.UNI_ENCODE_TYPE_OLAP && encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC)
				throw new Exception ("Invalid encoding type in header for unary datasets ");
			
			if (datasize[0] != 4)
				throw new Exception ("Invalid datasize in header for unary datasets ");
			
			// get the record count
			this._segmentCount = segmentCount[0];
			System.out.println("Segment count from CheckSum: " + this._segmentCount);
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 2 ||
					encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 0)
				throw new Exception ("Invalid decimal in header for unary document datasets ");
			
			
			this._dataLength = datasize[0]; // This is hardcoded as unary sets will store only integers
			this._decimals = decimals[0];
			this._encoding = encoding[0];
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC) {
				this._messageTypeSize = 2;
				if (messageType <= 0 || messageType > Short.MAX_VALUE)
					throw new Exception ("Invalid message type, message type must be between 1 and " + Short.MAX_VALUE);
				this._messageType = messageType;
			}
			else
				this._messageTypeSize = 0;
			
			this._currentRecordCount = (int)(f.length() - CheckSum.FILE_CHECKSUM_LENGTH)/(this._messageTypeSize + this._dataLength + this._decimals);
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * Constructor used when creating a new dataset.
	 * 
	 * @param dbName database name
	 * @param datasetName dataset name
	 * @param datasetType dataset type - UNI OLAP or UNI Document
	 * @param segmentCount segment count, max number of records allowed in this dataset
	 * @throws Exception
	 */
	public UniDataWriter(String dbName, String datasetName, int datasetType, int segmentCount, short messageType, int segmentNo) throws Exception {
		// TODO Auto-generated constructor stub
		int maxCount = 0;
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._datasetName = datasetName;
		
		File f = new File(this._datasetName);
		if (f.exists())
			throw new Exception("File " + this._datasetName + " already exists! Instantiate class with the non-creation constructor");
		
		if (segmentNo < 0)
			throw new Exception("Segment number cannot be < 0");
		
		this._segmentNo = segmentNo;
		
		if (datasetType != CheckSum.UNI_ENCODE_TYPE_OLAP && datasetType != CheckSum.UNI_ENCODE_TYPE_DOC)
			throw new Exception ("Invalid encoding type specified for unary datasets ");
		
		this._encoding = datasetType;
		this._dataLength = 4;
		
		if (datasetType == CheckSum.UNI_ENCODE_TYPE_OLAP) {
			this._decimals = 0;
			this._messageTypeSize = 0;
		}
		else if (datasetType == CheckSum.UNI_ENCODE_TYPE_DOC) {
			this._decimals = 2;
			this._messageTypeSize = 2;
			
			if (messageType <= 0 || messageType > Short.MAX_VALUE)
				throw new Exception ("Invalid message type, message type must be between 1 and " + Short.MAX_VALUE);
			this._messageType = messageType;
		}
		
		maxCount = HASIDSConstants.DIM_MAX_RECORDS/(this._messageTypeSize + this._dataLength + this._decimals);
		if (segmentCount < 0 || segmentCount > maxCount)
			throw new Exception("Invalid record count, record count must be > 0 and <= " + maxCount);
		
		// set the current record count in this dataset
		this._currentRecordCount = 0;
		
		// generate the session Id for this instance of the write class
		this._sessionId = System.nanoTime();
					
		try {
			// create the segment
			this.createSegment(segmentCount);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		
		// set the segment count
		this._segmentCount = segmentCount;
	}
	
	/**
	 * Create a segment with the segment size. No allocation for records as this is an expanding file.
	 * 
	 * @param segmentCount The maximum count of records allowed in this segment counted from 1
	 * @throws Exception
	 */
	private synchronized void createSegment(int segmentCount) throws Exception {
		this._status = UniDataWriter.ACTIVE;
		//byte buf = 0;
		
		long beginTime = System.nanoTime();
		
		try {
			File f = new File(this._datasetName);
			if (f.exists())
				throw new Exception("File " + this._datasetName + " exists! Cannot create segment");
			
			RandomAccessFile randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			FileChannel rwChannel = randomAccessFile.getChannel();
			MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, CheckSum.UNIFILE_CHECKSUM_LENGTH);
			
			// Write the CL at the beginning of the file
			buffer.position(0); // reset to position 0
			buffer.put((byte)this._fileType); // write the file type - UNI 3
			buffer.put((byte)this._encoding); // write the encoding type
			buffer.putInt(this._segmentNo); // write the segment number
			buffer.putInt(this._dataLength);
			buffer.putShort(this._decimals);
			
			System.out.println("File Type Written : " + this._fileType);
			System.out.println("Encoding Written : " + this._encoding);
			System.out.println("Segment Number Written : " + this._segmentNo);
			System.out.println("Data Length/Size Written: " + this._dataLength);
			System.out.println("Decimals Written : " + this._decimals);
			
			// write the dataset name and data set size
			buffer.put(CheckSum.computeCS(this._dbName + "|" + this._datasetName, CheckSum.UNIFILE_CHECKSUM_LENGTH));
			
			// calculate the current time
			long lastModifiedTime = System.currentTimeMillis();
			
			// Add it to the buffer
			buffer.put(Long.toString(CheckSum.computeTFS(lastModifiedTime)).getBytes());
			
			buffer.putInt(segmentCount); // write the segment count
			buffer.force();
			rwChannel.close();
			randomAccessFile.close();
			
			// set the last modified time
			f.setLastModified(lastModifiedTime);
			
			System.out.println("File " + this._datasetName + " length = " + f.length());
			
		}
		catch(Exception e) {
			this._status = UniDataWriter.FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == UniDataWriter.ACTIVE)
				this._status = UniDataWriter.COMPLETE;
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
            System.out.println("Segment creation time for : = " + elapsedTimeInMillis + "  Milliseconds");
            
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

	public String getDbName() {
		return _dbName;
	}

	public int getDataLength() {
		return _dataLength;
	}

	public int getEncoding() {
		return _encoding;
	}

	public int getStatus() {
		return _status;
	}

	public String getDatasetName() {
		return this._datasetName;
	}
	
	public int getRecordCount() {
		return this._currentRecordCount;
	}
	
	public int getSegmentCount() {
		return this._segmentCount;
	}
	
	public int getSegmentNo() {
		return this._segmentNo;
	}
	
	public long getSessionId() {
		return this._sessionId;
	}
	
	/**
	 * Method to check the correctness of the positions and values for writing the record ids
	 * and the counts to the segment. This is applicable only to document datasets.
	 * 
	 * @param recordIds An array of record ids to be written to the dataset
	 * @param counts An array of corresponding counts of occurrences associated with the record id
	 * @return
	 * @throws Exception
	 */
	private boolean checkPositionsValues(int[] recordIds, short[] counts) throws Exception {
		boolean writeStatus = true;
		
		if (recordIds == null || recordIds.length <= 0)
			throw new Exception("Record identifiers to be set must be > 0");
		
		if (this._encoding == CheckSum.UNI_ENCODE_TYPE_DOC) {
			if (counts == null || counts.length <= 0)
				throw new Exception("Counts to be set must be > 0");
			
			if (recordIds.length != counts.length)
				throw new Exception("Record id length and counts length do not match!");
		}
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input record ids count : " + recordIds.length);
		System.out.println("Max record count : " + this._segmentCount);
		System.out.println("Current record count : " + this._currentRecordCount);
		
		if (this._currentRecordCount + recordIds.length > this._segmentCount)
			throw new Exception("Number of record ids being added exceed the segment size");
		
		// Determine the low and high ranges so that the precise mapping range can be established
		int i = -1;
		int maxCountValue = Short.MAX_VALUE;
		for (i = 0; i < recordIds.length; i++)
		{
			if (recordIds[i] < 0 || recordIds[i] > this._segmentCount)
				throw new Exception("Positions to be set should be >= 0 and <= " + this._segmentCount);
			
			if (this._encoding == CheckSum.UNI_ENCODE_TYPE_DOC)
				if (counts[i] < 0 || counts[i] > maxCountValue)
					throw new Exception("counts to be set should be >= 0 and <= " + maxCountValue);
		}
		
		return writeStatus;
	}

	/**
	 * Method to write the record ids to the dataset associated with OLAP structures.
	 * @throws Exception
	 */
	private synchronized void writeToSegmentIds() throws Exception {
		int count = this._recordIds.length;
		for (int i = 0; i < count; i++)
			this._tempBuffer.putInt(this._recordIds[i]);
	}
	
	/**
	 * Method to write the record ids and corresponding counts to the datasets, associated with documents 
	 * @throws Exception
	 */
	private synchronized void writeToSegmentIdsCounts() throws Exception {
		int count = this._recordIds.length;
		for (int i = 0; i < count; i++) {
			this._tempBuffer.putShort(this._messageType);
			this._tempBuffer.putInt(this._recordIds[i]);
			this._tempBuffer.putShort(this._counts[i]);
		}
	}
	
	/**
	 * Method to set the record ids for writing the record ids to the segment. 
	 * This is applicable only to OLAP datasets.
	 * 
	 * @param recordIds An array of record ids to be written into the dataset.
	 * @throws Exception
	 */
	public void setWriteDataPositions(int[] recordIds) throws Exception {
		
		long beginTime = System.nanoTime();
	
		if (this._encoding == CheckSum.UNI_ENCODE_TYPE_DOC)
			throw new Exception("Counts buffer missing for document type unary sets");
		
		if (!this.checkPositionsValues(recordIds, null))
			throw new Exception ("Validation of record ids failed");
		
		this._recordIds = recordIds;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store record ids in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	/**
	 * 
	 * Method to set the record ids and corresponding counts in the dataset.
	 * 
	 * @param recordIds An array of record ids to be written to the dataset.
	 * @param counts An array of corresponding counts of occurrences associated with the record id
	 * @throws Exception
	 */
	public void setWriteDataPositions(int[] recordIds, short[] counts) throws Exception {
		
		long beginTime = System.nanoTime();
	
		if (this._encoding != CheckSum.UNI_ENCODE_TYPE_DOC)
			throw new Exception("Cannot set counts for non document type unary sets");
		
		this.checkPositionsValues(recordIds, counts);
		
		this._recordIds = recordIds;
		this._counts = counts;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store record ids and counts in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	

	
	/**
	 * Method to write the data set using the setter method to the dataset
	 * 
	 * @throws Exception
	 */
	public synchronized void writeToSegment(boolean autoCommit) throws Exception {
		
		this._status = UniDataWriter.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			//if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
			//	throw new Exception("Null Data or data length does not match values length! No data to write");
			
			File f = new File(this._datasetName);
			this._startPoint = (int)f.length();
			this._endPoint = this._startPoint + (this._recordIds.length * (this._messageTypeSize + this._dataLength + this._decimals));
			
			System.out.println("Write to segment, file length = " + this._startPoint);
			
			// create random file
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			// get file channel
			this._rwChannel = _randomAccessFile.getChannel();
			
			//Path path = FileSystems.getDefault().getPath(this._datasetName);
			//SeekableByteChannel dataFileChannel = Files.newByteChannel(path, EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
			//((FileChannel) dataFileChannel).map(FileChannel.MapMode.READ_WRITE, 100, 100);
			// map file to memory
			//MappedByteBuffer writeBuffer = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, FILE_SIZE, BUFFER_SIZE);
			this._tempBuffer = ByteBuffer.allocate((this._recordIds.length * (this._messageTypeSize + this._dataLength + this._decimals)));
			this._tempBuffer.order(ByteOrder.LITTLE_ENDIAN);
			//this._buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, this._startPoint, (this._recordIds.length * (this._messageTypeSize + this._dataLength + this._decimals)));
			// set the byte order to little endian
			//this._buffer.order(ByteOrder.LITTLE_ENDIAN);
			
			
			if (this._encoding != CheckSum.UNI_ENCODE_TYPE_DOC)
				this.writeToSegmentIds();
			else
				this.writeToSegmentIdsCounts();
			
			// commit
			if (autoCommit == true)
				this.commit();
			
		}
		catch(Exception e) {
			this._status = UniDataWriter.FAILED;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == UniDataWriter.ACTIVE)
				this._status = UniDataWriter.COMPLETE;
			
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
	public synchronized void commit() throws Exception {
		
		
		
		this._buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, this._startPoint, (this._recordIds.length * (this._messageTypeSize + this._dataLength + this._decimals)));
		// set the byte order to little endian
		this._buffer.order(ByteOrder.LITTLE_ENDIAN);
		this._buffer.put(this._tempBuffer.array());
					
		_buffer.force();
		
		// Write the TS
		long lastModifiedTime = this.writeCSTS();
				
		_rwChannel.close();
		_randomAccessFile.close();
		
		_tempBuffer.clear();
		_tempBuffer = null;
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
			MappedByteBuffer buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, CheckSum.FILE_DATASET_SIZE_POS, (CheckSum.FILE_DATASET_SIZE_LEN + CheckSum.FILE_DATASET_TIME_LEN));
			buffer.put(Long.toString(CheckSum.computeTFS(this._endPoint)).getBytes());
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
			this.writeToSegment(false);
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int segmentSize = 500000000;
		int runCount = segmentSize/1;
		int segmentNo = 0;
		short messageType = 1;
		UniDataWriter docWriter = null;
		try {
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\samsung_1.UN";
			
			
			
			/*int[] recordIds = new int[runCount];
			for (int i = 0; i < runCount; i++)
				recordIds[i] = i;
			
			// create the unary dataset
			UniDataWriter udw = new UniDataWriter(dbName, datasetName, CheckSum.UNI_ENCODE_TYPE_OLAP, segmentSize, (short)0);
			udw.setWriteDataPositions(recordIds);
			
			udw.writeToSegment();*/
			
			segmentSize = 10000000;
			runCount=100;
			datasetName = "c:\\users\\dpras\\tempdata\\testword_1.DC";
			int[] positions = new int[runCount];
			short[] counts = new short[runCount];
			
			for(int i = 0; i < runCount; i++) {
				positions[i] = (i * 100) + 1;
				counts[i] = (short) (positions[i]/100 + 1);
			}
			
			File f = new File(datasetName);
			if (!f.exists())
				docWriter = new UniDataWriter(dbName, datasetName, CheckSum.UNI_ENCODE_TYPE_DOC, segmentSize, messageType, segmentNo);
			else
				docWriter = new UniDataWriter(dbName, datasetName, messageType);
			
			docWriter.setWriteDataPositions(positions, counts);
			docWriter.writeToSegment(false);
			//docWriter.rollback();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
