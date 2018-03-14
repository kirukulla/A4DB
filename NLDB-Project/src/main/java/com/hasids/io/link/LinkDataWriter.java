/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */
package com.hasids.io.link;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 *
 * This class is the link set ancestor writer. Both 1:1 (UNCHAINED) and 1:n(CHAINED) relationships are 
 * supported for writing into link sets. 
 */
public abstract class LinkDataWriter implements Runnable {
	
	/*lock - begin transaction, update/write, retry success, update/write, unlock - end transaction, commit*/
	/*lock - begin transaction, update/write, retry failure, rollback - update/write original, unlock - end transaction*/
	
	// Data write buffer that is static
	private static Hashtable<String, Hashtable<Integer, Object>> LOCK_TABLE = new Hashtable<String, Hashtable<Integer, Object>>();;
	
	// The position table associated with a dataset stored within the LOCK_TABLE
	Hashtable<Integer, Object> _positionTable = null;
	
	// The position and values table for setting data values
	Hashtable<Integer, Object> _posValuesTable = null;
	
	// instance variables determining whether partial updates are allowed
	// if partial updates are not allowed, then an exception will be thrown back
	// in case any of the records are locked by another session
	// if partial updates are allowed then those ids that are unlocked are updated
	private boolean _allowPartial = false;
	
	// retry flag, this flag indicates if those records not available due to lock
	// by a different instance must be retried for updates after the rest are complete
	private Hashtable<Integer, Object> _retryTable = new Hashtable<Integer, Object>();
	
	// retry flag that determines if records that could not be written must be retried
	// for updates.
	private boolean _retryFlag = false; 
	
	/*Writing in raw mode without any synchronization*/
	private int _operationalMode = HASIDSConstants.OPERATION_MODE_BATCH;
	
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
	private int[] _position; //position
	private int[] _segments; // segments
	private int[] _values; // values
	private long[] _valuesLong; // values
	
	private int _positionsLength = 0;
	
	public static final short INACTIVE = 0;
	public static final short ACTIVE = 1;
	public static final short COMPLETE = 2;
	public static final short FAILED = 3;
	private int _status = LinkDataWriter.INACTIVE;
	
	byte[] _nullBuffer = null;
	byte _zeroByte = (byte)0;

	private byte[] _valuesByte;

	private short[] _valuesShort;

	private float[] _valuesFloat;

	private double[] _valuesDouble;

	private String[] _valuesString;
	
	
	/**
	 * Default constructor for updates
	 * @param dbName Database name
	 * @param datasetName Dataset name
	 * @param operationMode Operation mode - Batch/ONLINE
	 * @throws Exception
	 */
	public LinkDataWriter(String dbName, String datasetName, int operationMode) throws Exception {
		// TODO Auto-generated constructor stub
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
			int[] datasize = new int[1];
			short[] decimals = new short[1];
			int[] segmentNo = new int[1];
			
			CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			this._recordCount = (((int)f.length()) - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
			System.out.println("Record count from CheckSum: " + this._recordCount);
			
			this.checkEncodingExisting(fileType[0], encoding[0], datasize[0], decimals[0], segmentNo[0]);
			
			// initialize the null byte buffer
			initializeNullBuffer();
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * Default constructor for creating segments and writing data into the segment.
	 * 
	 * @param dbName Database name
	 * @param datasetName Dataset name
	 * @param recordCount Segment size
	 * @param encoding Encoding
	 * @param dataLength Data length
	 * @param decimals No of decimal positions
	 * @param segmentNo Segment Number
	 * 
	 * @throws Exception
	 */
	public LinkDataWriter(String dbName, String datasetName, int recordCount, int encoding, int dataLength, short decimals, int segmentNo) throws Exception {
		
		this._operationalMode = HASIDSConstants.OPERATION_MODE_BATCH;
		
		this.setFileType();
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		
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
		
		// initialize the null buffer
		initializeNullBuffer();
	}
	
	private void initializeNullBuffer() {
		// initialize the null byte buffer
		this._nullBuffer = new byte[this._dataLength - 1];
					
		for (int i = 0; i < this._dataLength - 2; i++)
			this._nullBuffer[i] = this._zeroByte;
	}
	
	protected void setFileType() {
		this._fileType = CheckSum.FILE_TYPE_LINK;
	}
	
	/*
	protected final void setDataLength(int dataLength) {
		this._dataLength = dataLength;
	}
	
	protected final void setEncoding(int encoding) {
		this._encoding = encoding;
	}
	
	protected final void setDecimals(short decimals) {
		this._decimals = decimals;
	}
	
	protected final void setSegmentNo(int segmentNo) {
		this._segmentNo = segmentNo;
	}
	*/
	
	private void checkEncodingNew(int encoding, int recordCount, int dataLength, short decimals, int segmentNo) throws Exception {
		// check segment
		if (segmentNo < 0)
			throw new Exception ("Segment number cannot be < 0!");
		
		this._segmentNo = segmentNo;
		
		// check File Type
		if (this._fileType == CheckSum.FILE_TYPE_LINK) {
			if (encoding != CheckSum.LINK_ENCODE_TYPE_UNCHAINED && encoding != CheckSum.LINK_ENCODE_TYPE_CHAINED)
				throw new Exception ("Link datasets data encoding type must be >= " + 
					CheckSum.LINK_ENCODE_TYPE_UNCHAINED + " and <= " + CheckSum.LINK_ENCODE_TYPE_UNCHAINED);
			else {
				this._decimals = 0;
				if (encoding == CheckSum.LINK_ENCODE_TYPE_UNCHAINED) {
					this._dataLength = 8; // Long
					if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS/this._dataLength) // 2 billion bytes
						throw new Exception ("Record count : " + recordCount + " must be > 0 and <= " + (HASIDSConstants.DIM_MAX_RECORDS/this._dataLength));
				}
				else if (encoding == CheckSum.LINK_ENCODE_TYPE_CHAINED) {
					// 1st four bytes represents from link at the segment level
					// Next eight bytes represents the to link across segment level and hence
					// will be a long
					// Next eight bytes represents the position of the next to link associated
					// the original from link.
					// E.g. CUSTOMER --> ORDERS
					// The first line will represent a list of original customer ids with one instance of the order
					// This will enable all the 
					this._dataLength = 20;
				}
			}
		}
		else
			throw new Exception ("Invalid file type received for creation!");
		
		System.out.println("Encoding: " + this._encoding + ", Check Length : " + this._dataLength);
		
		if (encoding == CheckSum.LINK_ENCODE_TYPE_UNCHAINED && (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS/this._dataLength)) // 2 billion bytes
			throw new Exception ("Record count : " + recordCount + " must be > 0 and <= " + (HASIDSConstants.DIM_MAX_RECORDS/this._dataLength));
		
		this._encoding = encoding;
		this._recordCount = recordCount;
	}
	
	private void checkEncodingExisting(int fileType, int encoding, int datasize, short decimals, int segmentNo) throws Exception {

		if (segmentNo < 0)
			throw new Exception ("Segment number cannot be < 0!");
		
		this._segmentNo = segmentNo;
		
		if (fileType == CheckSum.FILE_TYPE_FACT) {
			if (encoding < CheckSum.FACT_ENCODE_TYPE_BYTE || encoding > CheckSum.FACT_ENCODE_TYPE_ALPHAN)
				throw new Exception ("Fact datasets data encoding type must be >= " + 
					CheckSum.FACT_ENCODE_TYPE_BYTE + " and <= " + CheckSum.FACT_ENCODE_TYPE_ALPHAN);
			else {
				if (encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)
					if (datasize != 1)
						throw new Exception ("Invalid data length in file header!");
				else if (encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)
					if (datasize != 2)
						throw new Exception ("Invalid data length in file header!");
				else if (encoding == CheckSum.FACT_ENCODE_TYPE_INT || encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT)
					if (datasize != 4)
						throw new Exception ("Invalid data length in file header!");
				else if (encoding == CheckSum.FACT_ENCODE_TYPE_LONG || encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE)
					if (datasize != 8)
						throw new Exception ("Invalid data length in file header!");
				else if (encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN)
					if (datasize <= 0 || datasize > (HASIDSConstants.FACT_MAX_ALPHAN_LENGTH + 1))
						throw new Exception ("Max data length allowed in bytes is " + HASIDSConstants.FACT_MAX_ALPHAN_LENGTH + ", Invalid length in file header");
				else if (encoding == CheckSum.FACT_ENCODE_TYPE_DECIMAL) { // stored as aplha numeric decimals rounded
					if (decimals > datasize)
						throw new Exception ("Decimal type fact datasets cannot have decimal positions > data length");
				
					if (datasize <= 0)
						throw new Exception ("Invalid data length in file header!");
					
					this._dataLength = datasize;
					this._decimals = decimals;
				}
			}
		}
		else
			throw new Exception ("Invalid file type received for creation!");
		
		this._dataLength = datasize;
		this._encoding = encoding;
		this._decimals = decimals;
	}

	
	/**
	 * Create a segment with the record size. Each byte in the segment is set to null value
	 * to begin with. Null is ASCII code zero. This is a synchronized method. It ensures that 
	 * no two threads create the same segment
	 * 
	 * @throws Exception
	 */
	private void createSegment() throws Exception {
		this._status = LinkDataWriter.ACTIVE;
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
			buffer.putInt(this.getSegmentNo()); // write the segment number
			buffer.putInt(this.getDataLength()); // write the data length
			buffer.putShort(this.getDecimals()); // write the decimal length
			
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
			this._status = LinkDataWriter.FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == LinkDataWriter.ACTIVE)
				this._status = LinkDataWriter.COMPLETE;
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
	
	private boolean checkPositionsValues(int[] position, String[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		int minValue = 1;
		int maxValue = this._dataLength;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		
		int strLen = -1;
		byte byte0 = 0;
		char c0 = (char)byte0;
		for (int i = 0; i < position.length; i++)
		{
			if (position[i] < 0 || position[i] > this._recordCount - 1) {
				writeStatus = false;
				message = "Positions to be set should be >= 0 and < record count";
				break;
			}
			
			// Durga Turaga, 12/06/2017, added check condition for null and setting null string
			if (values[i] != null && values[i].length() > 0) {
				strLen = values[i].getBytes().length;
				if (strLen < minValue || strLen > maxValue) {
					writeStatus = false;
					message = "Values to be set should be >= " + minValue + " and <= " + maxValue + "; 0 is reserved to mean no change";
					break;
				}
			}
			else
				values[i] = new String(this._nullBuffer);
			
			// pad the strings to their max allowable values
			// IS THIS THE BEST WAY TO DO THIS?
			// DONT DO THIS ON AN INSERT; DO IT ON AN UPDATE ONLY
			// UPDATES WILL HAPPEN IN ONLINE MODE ONLY??
			// PADDING REMOVED - Durga Turaga, 12/06/2017, instead the actual length
			// will be stored in the first byte
			/*if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
				if (strLen < maxValue) {
					StringBuffer sb = new StringBuffer(maxValue).append(values[i]);
					
					for(int j = strLen + 1; j <= maxValue; j++) {
						//System.out.println("Current position in array fill : " + j);
						sb.append(c0);
					}
					values[i] = sb.toString();
					//System.out.println("Position : " + i + ", value : " + values[i]);
				}
			}*/
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
		
		return writeStatus;
	}

	private boolean checkPositionsValues(int[] position, byte[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		byte minValue = HASIDSConstants.DIM_ENCODE_TYPE1_MIN;
		byte maxValue = HASIDSConstants.DIM_ENCODE_TYPE1_MAX;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}
	
	private boolean checkPositionsValues(int[] position, short[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		short minValue = HASIDSConstants.DIM_ENCODE_TYPE2_MIN;
		short maxValue = HASIDSConstants.DIM_ENCODE_TYPE2_MAX;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}
	
	private boolean checkPositionsValues(int[] position, int[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		int minValue = HASIDSConstants.DIM_ENCODE_TYPE3_MIN;
		int	maxValue = HASIDSConstants.DIM_ENCODE_TYPE3_MAX;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}

	private boolean checkPositionsValues(int[] position, float[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		float minValue = Float.MIN_VALUE + 0.1f;
		float maxValue = Float.MAX_VALUE;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}
	
	private boolean checkPositionsValues(int[] position, long[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		long minValue = Long.MIN_VALUE + 1;
		long maxValue = Long.MAX_VALUE;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}

	private boolean checkPositionsValues(int[] position, double[] values) throws Exception {
		boolean writeStatus = true;
		
		if (position == null || values == null)
			throw new Exception("Positions and values cannot be null");
		
		if (position.length != values.length)
			throw new Exception("Positions and values not of identical length!");
		
		if (position.length > this._recordCount)
			throw new Exception("No of positions received to write > file record count");
		
		double minValue = Double.MIN_VALUE + 0.1;
		double maxValue = Double.MAX_VALUE;
		
		System.out.println(this._datasetName);
		System.out.println("Data Length : " + this._dataLength);
		System.out.println("Input positions/values : " + position.length + "/" + values.length);
		System.out.println("MIX/MAX Values allowable : " + minValue + "/" + maxValue);
		
		// Determine the low and high ranges so that the precise mapping range can be established
		String message = null;
		for (int i = 0; i < position.length; i++)
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
		}
		
		System.out.println("Low range : " + this._lowRange);
		System.out.println("High range : " + this._highRange);
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus)
			throw new Exception (message);
		
		return writeStatus;
	}
	
	private void setWriteDataPositionBufferBatch(int[] position, byte[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("BATCH : " + this._sessionId);
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesByte = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}

	private void setWriteDataPositionBufferBatch(int[] position, short[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("BATCH : " + this._sessionId);
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesShort = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
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
	private void setWriteDataPositionBufferBatch(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("BATCH : " + this._sessionId);
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._values = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}

	private void setWriteDataPositionBufferBatch(int[] position, float[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesFloat = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	private void setWriteDataPositionBufferBatch(int[] position, long[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesLong = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	private void setWriteDataPositionBufferBatch(int[] position, double[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesDouble = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	private void setWriteDataPositionBufferBatch(int[] position, String[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("BATCH : " + this._sessionId);
		
		long beginTime = System.nanoTime();
	
		this.checkPositionsValues(position, values);
		
		this._valuesString = values;
		this._position = position;
		this._positionsLength = position.length;
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
	}
	
	private Hashtable<Integer, Byte> setWriteDataPositionBufferOnline(int[] position, byte[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("ONLINE : " + this._sessionId);
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Byte> sReturn = new Hashtable<Integer, Byte>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new byte[] {values[i], this._buffer.get((position[i] - this._lowRange))});
						
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
					
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag) {
						this._retryTable.put(position[i], values[i]);
						
						System.out.println("Added to retry table : " + this._sessionId);
					}
				}			
			}
        }
        
		System.out.println("No of entries in position table : " + this._positionTable.size());
		System.out.println("No of entries in pos values table : " + this._posValuesTable.size());
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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

	private Hashtable<Integer, Short> setWriteDataPositionBufferOnline(int[] position, short[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("ONLINE : " + this._sessionId);
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Short> sReturn = new Hashtable<Integer, Short>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new short[] {values[i], this._buffer.getShort((position[i] - this._lowRange) * this._dataLength)});
						
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
					
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag) {
						this._retryTable.put(position[i], values[i]);
						
						System.out.println("Added to retry table : " + this._sessionId);
					}
				}			
			}
        }
        
		System.out.println("No of entries in position table : " + this._positionTable.size());
		System.out.println("No of entries in pos values table : " + this._posValuesTable.size());
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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
	 * Method that stores the data positions (record ids) and values to be written to the
	 * file. This is a synchronized method. Record ids that are being written are locked
	 * first, retry table entries populated if ids are locked by a different session. 
	 * 
	 * @param position An array of ids whose values must be set and are in ascending order
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	private Hashtable<Integer, Integer> setWriteDataPositionBufferOnline(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		System.out.println("ONLINE : " + this._sessionId);
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Integer> sReturn = new Hashtable<Integer, Integer>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new int[] {values[i], this._buffer.getInt((position[i] - this._lowRange) * this._dataLength)});
						
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag) {
						this._retryTable.put(position[i], values[i]);
						
						System.out.println("Added to retry table : " + this._sessionId);
					}
				}			
			}
        }
        
		System.out.println("No of entries in position table : " + this._positionTable.size());
		System.out.println("No of entries in pos values table : " + this._posValuesTable.size());
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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
	
	private Hashtable<Integer, Float> setWriteDataPositionBufferOnline(int[] position, float[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Float> sReturn = new Hashtable<Integer, Float>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new float[] {values[i], this._buffer.getFloat((position[i] - this._lowRange) * this._dataLength)});
					
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag)
						this._retryTable.put(position[i], values[i]); 
				}			
			}
        }
        
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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

	private Hashtable<Integer, Long> setWriteDataPositionBufferOnline(int[] position, long[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Long> sReturn = new Hashtable<Integer, Long>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new long[] {values[i], this._buffer.getLong((position[i] - this._lowRange) * this._dataLength)});
					
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag)
						this._retryTable.put(position[i], values[i]); 
				}			
			}
        }
        
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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
	
	private Hashtable<Integer, Double> setWriteDataPositionBufferOnline(int[] position, double[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Double> sReturn = new Hashtable<Integer, Double>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
			for (i = 0; i < position.length; i++)
			{
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new double[] {values[i], this._buffer.getDouble((position[i] - this._lowRange) * this._dataLength)});
					
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag)
						this._retryTable.put(position[i], values[i]); 
				}			
			}
        }
        
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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

	private Hashtable<Integer, String> setWriteDataPositionBufferOnline(int[] position, String[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, String> sReturn = new Hashtable<Integer, String>();

		boolean writeStatus = true;
		String message = null;
		int i = -1;
		
		writeStatus = this.checkPositionsValues(position, values);		
		beginTime = System.nanoTime();
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;

		synchronized (this) {
			// Position table; hashtable of positions and values associated with multiple sessions of this dataset
			this._positionTable = LinkDataWriter.LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			if (this._positionTable == null) {
				this._positionTable = new Hashtable<Integer, Object>(position.length);
				LinkDataWriter.LOCK_TABLE.put(this._dbName + "|" + this._datasetName, this._positionTable);
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
		this._posValuesTable = new Hashtable<Integer, Object>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1) * this._dataLength);
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
        
        // Durga Turaga, 12/06/2017, removed hard coding
        // data length must be dynamically read
        byte[] bb;
        short dataLen = 0;
        synchronized (this._positionTable) {
			for (i = 0; i < position.length; i++)
			{
				bb = null;
				this._buffer.position((position[i] - this._lowRange) * this._dataLength);
				// Durga Turaga, 12/06/2017, skip one position for the data length
				
				// get the length of the data
				dataLen = this._buffer.get();
				if (dataLen < 0)
					dataLen += 256;
				
				// only if the original data is not null, do we need to read it
				if (dataLen > 0) {
					// instantiate the buffer
					bb = new byte[dataLen];
					this._buffer.get(bb);
				}
				
				//if (i % 100000 == 0)
				//	System.out.println("Position : " + position[i]);
				
				// Add the record ids to the position table if the record id is not locked
				// Array 0 - new value, array 1 = old value
				if (!this._positionTable.containsKey(position[i])) {
					
					// lock the position by adding it to the position table
					this._positionTable.put(position[i], new String[] {values[i], (dataLen > 0 ? new String(bb) : new String(this._nullBuffer))});
					
					// add the position and value to the pos values table; this will be used to 
					// actually write to the file
					this._posValuesTable.put(position[i], values[i]);
				}
				else {
					if (!this._allowPartial && !this._retryFlag) {
						sReturn.put(position[i], values[i]);
						writeStatus = false;
						message = "Records locked for updating by another session";
						break;
					}
								
					// if retry flag is true, add the positions to the retry table
					if (this._retryFlag)
						this._retryTable.put(position[i], values[i]); 
				}			
			}
        }
        
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to set the position and posValues tables in memory : " + elapsedTimeInMillis + "  Milliseconds");
        System.out.println("No of entries in the retry table : " + this._retryTable.size());
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
			LOCK_TABLE.remove(this._datasetName);
		
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
				this._buffer.put((this._position[i] - this._lowRange), this._valuesByte[i]);
	
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//byte value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.put((i - this._lowRange) * this._dataLength, ((Byte)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
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
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(shortBytes);
				this._buffer.putShort(this._valuesShort[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//short value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.putShort((i - this._lowRange) * this._dataLength, ((Short)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
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
			
			//int value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				//value = ((int)this._posValuesTable.get(i));
				
				//System.out.println("Setting position : " + i + ", for a value of : " + ((Integer)this._posValuesTable.get(i)) + ", at position : " + (i - this._lowRange));
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.putInt((i - this._lowRange) * this._dataLength, ((Integer)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
			}
		}
	}
	
	private void writeToSegmentTypeFloat() throws Exception {
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//intBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(intBytes);
				this._buffer.putFloat(this._valuesFloat[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//int value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				//value = ((int)this._posValuesTable.get(i));
				
				//System.out.println("Setting position : " + i + ", for a value of : " + ((Float)this._posValuesTable.get(i)) + ", at position : " + (i - this._lowRange));
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.putFloat((i - this._lowRange) * this._dataLength, ((Float)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
			}
		}
	}
	
	private void writeToSegmentTypeLong() throws Exception {
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//intBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(intBytes);
				this._buffer.putLong(this._valuesLong[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//int value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				//value = ((int)this._posValuesTable.get(i));
				
				//System.out.println("Setting position : " + i + ", for a value of : " + ((Long)this._posValuesTable.get(i)) + ", at position : " + (i - this._lowRange));
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.putLong((i - this._lowRange) * this._dataLength, ((Long)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
			}
		}
	}
	
	private void writeToSegmentTypeDouble() throws Exception {
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//intBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				//this._buffer.put(intBytes);
				this._buffer.putDouble(this._valuesDouble[i]);
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//int value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				//value = ((int)this._posValuesTable.get(i));
				
				//System.out.println("Setting position : " + i + ", for a value of : " + ((Double)this._posValuesTable.get(i)) + ", at position : " + (i - this._lowRange));
				
				// write to dataset
				//this._buffer.position((i - this._lowRange) * this._dataLength);
				this._buffer.putDouble((i - this._lowRange) * this._dataLength, ((Double)this._posValuesTable.get(i)));
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
				
			}
		}
	}
	
	private void writeToSegmentTypeString() throws Exception {
		byte[] bb = null;
		int bbLen = 0;
		
		
		if (this._operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			for (int i = 0; i < _positionsLength; i++) {
				//intBytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(this._values[i]).array();
				this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				
				// Durga Turaga, 12/06/2017; write the data length first, then write the bytes
				bb = this._valuesString[i].getBytes();
				bbLen = bb.length;
				
				//System.out.println("Writing bytes of length : " + bbLen);
				// set the length of the number of bytes we are setting
				if (bb[0] == 0 && bb[bbLen - 1] == 0)
					// set zero length and null buffer
					this._buffer.put(this._zeroByte);
				else
					this._buffer.put((byte)bbLen);
					
				// put the bytes
				this._buffer.put(bb);
				
			}
		}
		else if (this._operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			String temp = null;
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._dbName + "|" + this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			
			//int value = 0;
			while (e.hasMoreElements()) {
				
				i = e.nextElement();
				
				//System.out.println("Setting position : " + i + ", for a value of : " + ((String)this._posValuesTable.get(i)) + ", at position : " + (i - this._lowRange));
				//this._buffer.position((this._position[i] - this._lowRange) * this._dataLength);
				
				// write to dataset
				this._buffer.position((i - this._lowRange) * this._dataLength);
				
				// Durga Turaga, 12/06/2017, write the data size first before writing the data
				bb = ((String)this._posValuesTable.get(i)).getBytes();
				bbLen = bb.length;

				//System.out.println("Writing bytes of length : " + bbLen);
				if (bb[0] == 0 && bb[bbLen - 1] == 0)
					// set zero length and null buffer
					this._buffer.put(this._zeroByte);
				else
					this._buffer.put((byte)bbLen);
				
				this._buffer.put(bb);
				//System.out.println("Position : " + (i - this._lowRange) + ", written a value of : " + value);
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
		
		System.out.println("Number of retries that will be attempted : " + iterations);
		
		// loop through the number of iterations
		for (int j = 0; j < iterations; j++) {
			
			System.out.println("Retry attempt : " + (j + 1));
			Integer i = null;
			
			// get the list of positions in the retry table
			Enumeration<Integer> e = this._retryTable.keys();
			if (this._dataLength == CheckSum.BYTE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				Byte b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = (Byte)this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new Byte[] {b, this._buffer.get((i - this._lowRange))});
					
						// write to the position in the memory buffer
						this._buffer.put((i - this._lowRange), b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == CheckSum.SHORT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				short b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					System.out.println(this._sessionId + ", Retrying position : " + i + ", " + (i - this._lowRange));
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = ((Short)this._retryTable.get(i));
					
						// lock the position 
						this._positionTable.put(i, new Short[] {b, this._buffer.getShort((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putShort((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == CheckSum.INT_FLOAT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				Integer b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = (Integer)this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new Integer[] {b, this._buffer.getInt((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putInt((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == CheckSum.INT_FLOAT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				Float b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = (Float)this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new Float[] {b, this._buffer.getFloat((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putFloat((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == CheckSum.LONG_DOUBLE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				Long b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = (Long)this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new Long[] {b, this._buffer.getLong((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putLong((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._dataLength == CheckSum.LONG_DOUBLE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				Double b;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the byte value
						b = (Double)this._retryTable.get(i);
					
						// lock the position 
						this._positionTable.put(i, new Double[] {b, this._buffer.getDouble((i - this._lowRange) * this._dataLength)});
					
						// write to the position in the memory buffer
						this._buffer.putDouble((i - this._lowRange) * this._dataLength, b);
					
						// add it to the pos values table for future commit/rollback
						this._posValuesTable.put(i, this._retryTable.remove(i));
					}
				}
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				String b;
				
				// Durga Turaga, 12/06/2017, remove the hard coding and instead use the input length
				byte[] bb = null;
				byte[] inputbb = null;
				short dataLen;
				int inputLen;
				while(e.hasMoreElements()) {
					i = e.nextElement();
					if (!this._positionTable.containsKey(i)) { // this position is not locked
						// get the string value
						b = (String)this._retryTable.get(i);
						inputbb = b.getBytes();
						inputLen = inputbb.length;
						
						this._buffer.position((i - this._lowRange) * this._dataLength);
						
						// get the length of the data
						dataLen = this._buffer.get();
						if (dataLen < 0)
							dataLen += 256;
						
						// only if the original data is not null, do we need to read it
						if (dataLen > 0) {
							// instantiate the buffer
							bb = new byte[dataLen];
							this._buffer.get(bb);
						}
						
						// lock the position 
						this._positionTable.put(i, new String[] {b, (dataLen > 0 ? new String(bb) : new String(this._nullBuffer))});
						
						this._buffer.position((i - this._lowRange) * this._dataLength);
						
						// write the length
						if (inputbb[0] == 0 && inputbb[inputLen - 1] == 0)
							// set zero length and null buffer
							this._buffer.put(this._zeroByte);
						else
							this._buffer.put((byte)inputLen);
						
						// write to the position in the memory buffer
						this._buffer.put(inputbb);
					
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
			
			if (this._dataLength == CheckSum.BYTE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
			
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.put((i - this._lowRange) * this._dataLength, 
							((byte[])this._positionTable.get(i))[1]);
				}
			}
			else if (this._dataLength == CheckSum.SHORT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.putShort((i - this._lowRange) * this._dataLength, 
							((short[])this._positionTable.get(i))[1]);
				}
			}
				
			else if (this._dataLength == CheckSum.INT_FLOAT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.putInt((i - this._lowRange) * this._dataLength, 
							((int[])this._positionTable.get(i))[1]);
				}
				
			}
			else if (this._dataLength == CheckSum.INT_FLOAT_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.putFloat((i - this._lowRange) * this._dataLength, 
							((float[])this._positionTable.get(i))[1]);
				}
			}
			else if (this._dataLength == CheckSum.LONG_DOUBLE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.putLong((i - this._lowRange) * this._dataLength, 
							((long[])this._positionTable.get(i))[1]);
				}
			}
			else if (this._dataLength == CheckSum.LONG_DOUBLE_LEN && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				int i = -1;
				
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.putDouble((i - this._lowRange) * this._dataLength, 
							((double[])this._positionTable.get(i))[1]);
				}
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				int i = -1;
				byte[] bb;
				int bbLen;
				while(it.hasNext()) {
					i = it.next();
					System.out.println("Rollback position : " + i);
			
					this._buffer.position((i - this._lowRange) * this._dataLength);
					
					// Durga Turaga, 12/06/2017, Add the data length and account for null string
					bb = ((String[])this._positionTable.get(i))[1].getBytes();
					bbLen = bb.length;
					
					if (bb[0] == 0 && bb[bbLen - 1] == 0)
						this._buffer.put(this._zeroByte);
					else
						this._buffer.put((byte)bbLen);
					
					this._buffer.put(bb);
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
	
	protected Hashtable<Integer, Byte> setWriteDataPositionBuffer(int[] position, byte[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Byte> retValue = new Hashtable<Integer, Byte>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}

	protected Hashtable<Integer, Short> setWriteDataPositionBuffer(int[] position, short[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Short> retValue = new Hashtable<Integer, Short>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
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
	protected Hashtable<Integer, Integer> setWriteDataPositionBuffer(int[] position, int[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Integer> retValue = new Hashtable<Integer, Integer>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	protected Hashtable<Integer, Float> setWriteDataPositionBuffer(int[] position, float[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Float> retValue = new Hashtable<Integer, Float>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	protected Hashtable<Integer, Long> setWriteDataPositionBuffer(int[] position, long[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Long> retValue = new Hashtable<Integer, Long>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	protected Hashtable<Integer, Double> setWriteDataPositionBuffer(int[] position, double[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, Double> retValue = new Hashtable<Integer, Double>();
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE)
			return this.setWriteDataPositionBufferOnline(position, values, allowPartial, retryFlag);
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH)
			this.setWriteDataPositionBufferBatch(position, values, allowPartial, retryFlag);
		
		return retValue;
	}
	
	protected Hashtable<Integer, String> setWriteDataPositionBuffer(int[] position, String[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("File encoding type mismatch with input data!");
		
		Hashtable<Integer, String> retValue = new Hashtable<Integer, String>();
		
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
	public final Set<Integer> deleteRecords(int[] position, boolean allowPartial, boolean retryFlag) throws Exception {

		Set<Integer> returnSet = null;
		
		if (position == null || position.length == 0)
			throw new Exception("Invalid records ids to delete! Use truncate for deleting all records!!");
		
		int count = position.length;
		Object o = null;
		
		if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
			byte[] values = new byte[count];
			for (int i = 0; i < count; i++)
				values[i] = Byte.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
			short[] values = new short[count];
			for (int i = 0; i < count; i++)
				values[i] = Short.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
			int[] values = new int[count];
			for (int i = 0; i < count; i++)
				values[i] = Integer.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
			float[] values = new float[count];
			for (int i = 0; i < count; i++)
				values[i] = Float.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
			long[] values = new long[count];
			for (int i = 0; i < count; i++)
				values[i] = Long.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
			double[] values = new double[count];
			for (int i = 0; i < count; i++)
				values[i] = Double.MIN_VALUE;
			o = values;
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
			String[] values = new String[count];
			
			// create a null byte buffer
			byte[] bb = new byte[this._dataLength];
			for (int i = 0; i < this._dataLength; i++)
				bb[i] = 0;
			
			// construct a null string (where all bytes are byte 0) from the byte buffer
			String s = new String(bb);
			for (int i = 0; i < count; i++)
				values[i] = s;
			o = values;
		}
		
		if (_operationalMode == HASIDSConstants.OPERATION_MODE_ONLINE) {
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				byte[] arr = (byte[]) o;
				Hashtable<Integer, Byte> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				short[] arr = (short[]) o;
				Hashtable<Integer, Short> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				int[] arr = (int[]) o;
				Hashtable<Integer, Integer> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				float[] arr = (float[]) o;
				Hashtable<Integer, Float> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				long[] arr = (long[]) o;
				Hashtable<Integer, Long> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				double[] arr = (double[]) o;
				Hashtable<Integer, Double> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				String[] arr = (String[]) o;
				Hashtable<Integer, String> h = this.setWriteDataPositionBufferOnline(position, arr, allowPartial, retryFlag);
				returnSet = h.keySet();
			}
			
		}
		else if (_operationalMode == HASIDSConstants.OPERATION_MODE_BATCH) {
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				byte[] arr = (byte[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				short[] arr = (short[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				int[] arr = (int[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				float[] arr = (float[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				long[] arr = (long[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				double[] arr = (double[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				String[] arr = (String[]) o;
				this.setWriteDataPositionBufferBatch(position, arr, allowPartial, retryFlag);
			}
			
		}
		
		return returnSet;
	}
	
	protected static Hashtable<Integer, Byte> getLockedKeysByte(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Byte> returnTable = new Hashtable<Integer, Byte>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Byte b = 0;
			byte[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (byte[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	protected static Hashtable<Integer, Short> getLockedKeysShort(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Short> returnTable = new Hashtable<Integer, Short>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Short b = 0;
			short[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (short[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	/***
	 * Method that returns the locked record identifiers and their original values in a Hashtable.
	 * 
	 * @param datasetName Dataset name for which record identifiers and original values in locked status are required
	 * @return Hashtable with keys of Integers and values of Bytes.
	 */
	protected static Hashtable<Integer, Integer> getLockedKeysInt(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Integer> returnTable = new Hashtable<Integer, Integer>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Integer b = 0;
			int[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (int[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	protected static Hashtable<Integer, Float> getLockedKeysFloat(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Float> returnTable = new Hashtable<Integer, Float>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Float b = 0f;
			float[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (float[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	protected static Hashtable<Integer, Long> getLockedKeysLong(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Long> returnTable = new Hashtable<Integer, Long>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Long b = 0l;
			long[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (long[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	protected static Hashtable<Integer, Double> getLockedKeysDouble(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, Double> returnTable = new Hashtable<Integer, Double>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			Double b = 0d;
			double[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (double[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
	}
	
	protected static Hashtable<Integer, String> getLockedKeysString(String dbName, String datasetName) {
		// create the return table
		Hashtable<Integer, String> returnTable = new Hashtable<Integer, String>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Object> positionTable = LinkDataWriter.LOCK_TABLE.get(dbName + "|" + datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i = 0;
			String b = null;
			String[] tempArray;
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				tempArray = (String[])positionTable.get(i);
				b = tempArray[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
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
		
		this._status = LinkDataWriter.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			//if (_position == null || _values == null || _position.length <= 0 || (_position.length != _values.length))
			//	throw new Exception("Null Data or data length does not match values length! No data to write");
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception ("File : " + this._datasetName + " does not exist!");
			
			// create random file
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			// get file channel
			this._rwChannel = _randomAccessFile.getChannel();
			// map file to memory
			this._buffer = _rwChannel.map(FileChannel.MapMode.READ_WRITE, ((this._lowRange * this._dataLength) + CheckSum.FILE_CHECKSUM_LENGTH), ((this._highRange - this._lowRange + 1) * this._dataLength));
			// set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
			// only if it is not ALPHAN
			if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
				this._buffer.order(ByteOrder.LITTLE_ENDIAN);
            
			// based on the data length we have to write the bytes
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)
				this.writeToSegmentTypeByte();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)
				this.writeToSegmentTypeShort();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)
				this.writeToSegmentTypeInt();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT)
				this.writeToSegmentTypeFloat();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG)
				this.writeToSegmentTypeLong();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE)
				this.writeToSegmentTypeDouble();
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN)
				this.writeToSegmentTypeString();
			
			// iterate through the positions and values and write to file
			//for (int i = 0; i < _position.length; i++)
			//	this._buffer.put((this._position[i] - this._lowRange), (byte)this._values[i]);
			
			// check for the retry table.
			if (this._retryTable.size() > 0) {
				this.retry();
			}
						
			// if we are here and if the autocommit flag is set, we can commit
			if (autoCommit)
				this.commit();
			
		}
		catch(Exception e) {
			this._status = LinkDataWriter.FAILED;
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == LinkDataWriter.ACTIVE)
				this._status = LinkDataWriter.COMPLETE;
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("File based read/write time : " + elapsedTimeInMillis + "  Milliseconds");
			
		}
	}	
	
	public void run() {
		// TODO Auto-generated method stub
		// when operating in a thread mode, always set auto commit to false
		// The initiator of the thread must call the commit
		try {
			this.writeToSegment(false);
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
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
			
			
			/*
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
			*/
			
			/*
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
			*/

			


			
			
			
			
			
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
