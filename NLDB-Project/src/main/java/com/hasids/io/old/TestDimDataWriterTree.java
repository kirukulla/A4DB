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
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 *
 * This class is a test writer component. It allows the creation of data segments 
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
 */
public class TestDimDataWriterTree implements Runnable {
	
	/*lock - begin transaction, update/write, retry success, update/write, unlock - end transaction, commit*/
	/*lock - begin transaction, update/write, retry failure, rollback - update/write original, unlock - end transaction*/
	
	// Data write buffer that is static
	private static Hashtable<String, SortedMap<Integer, Byte[]>> LOCK_TABLE = 
			new Hashtable<String, SortedMap<Integer, Byte[]>>();
	
	// The position table associated with a dataset stored within the LOCK_TABLE
	SortedMap<Integer, Byte[]> _positionTable = null;
	
	// The position and values table for setting data values
	SortedMap<Integer, Byte> _posValuesTable = Collections.synchronizedSortedMap(new TreeMap<Integer, Byte>());
	
	// instance variables determining whether partial updates are allowed
	// if partial updates are not allowed, then an exception will be thrown back
	// in case any of the records are locked by another session
	// if partial updates are allowed then those ids that are unlocked are updated
	private boolean _allowPartial = false;
	
	// retry flag, this flag indicates if those records not available due to lock
	// by a different instance must be retried for updates after the rest are complete
	private Hashtable<Integer, Byte> _retryTable = new Hashtable<Integer, Byte>();
	
	// retry flag that determines if records that could not be written must be retried
	// for updates.
	private boolean _retryFlag = false; 
	
	// instance variables associated with the dataset
	private String _datasetName;
	private int _recordCount;
	private RandomAccessFile _randomAccessFile;
	private FileChannel _rwChannel = null;
	private MappedByteBuffer _buffer = null;
	private long _sessionId = 0L;
	private int _lowRange = HASIDSConstants.DIM_MAX_RECORDS + 1;
	private int _highRange = -1;
	
	public static final short INACTIVE = 0;
	public static final short ACTIVE = 1;
	public static final short COMPLETE = 2;
	public static final short FAILED = 3;
	private int _status = TestDimDataWriterTree.INACTIVE;
	
	/**
	 * Constructor to use when writing into an existing file
	 * 
	 * @param datasetName Name of the dataset to write data into
	 * 
	 * @throws Exception when the file is not present
	 */
	public TestDimDataWriterTree(String datasetName) throws Exception {
		// TODO Auto-generated constructor stub
		this._datasetName = datasetName;
		try {
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			
			this._recordCount = (int)f.length();
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
			
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * The constructor to use when the dataset does not exist
	 * 
	 * @param datasetName Name of the dataset to create
	 * @param recordCount	Number of records in the dataset
	 * @throws Exception	If the file already exists
	 */
	public TestDimDataWriterTree(String datasetName, int recordCount) throws Exception {
		
		if (datasetName == null || datasetName.length() <= 0)
			throw new Exception ("Invalid dataset name");
		
		this._datasetName = datasetName;
		if (recordCount <= 0 || recordCount > HASIDSConstants.DIM_MAX_RECORDS) // 2 billion bytes
			throw new Exception ("Record count must be > 0 and <= " + HASIDSConstants.DIM_MAX_RECORDS);
		
		this._recordCount = recordCount;
		
		// generate the session Id for this instance of the write class
		this._sessionId = System.nanoTime();
				
		try {
			this.createSegment();
		}
		catch (Exception e) {
			//just catch the exception for now;
			// if segment is already there, we can ignore it.
			// if failure is because of something else, we 
		}
	}
	
	public String getDatasetName() {
		return this._datasetName;
	}
	
	public int getRecordCount() {
		return this._recordCount;
	}
	
	public boolean getPartialWriteFlag() {
		return this._allowPartial;
	}
	
	public boolean getRetryFlag() {
		return this._retryFlag;
	}
	
	public long getSessionId() {
		return this._sessionId;
	}
	
	/***
	 * Method that returns the locked record identifiers and their original values in a Hashtable.
	 * 
	 * @param datasetName Dataset name for which record identifiers and original values in locked status are required
	 * @return Hashtable with keys of Integers and values of Bytes.
	 */
	public static SortedMap<Integer, Byte> getLockedKeys(String datasetName) {
		// create the return table
		TreeMap<Integer, Byte> returnTable = new TreeMap<Integer, Byte>();
		
		// get the position table for the input dataset name
		SortedMap<Integer, Byte[]> positionTable = TestDimDataWriterTree.LOCK_TABLE.get(datasetName);
		if (positionTable != null) { // if we have a position table in memory
			Iterator<Integer> it = positionTable.keySet().iterator();
			int i;
			Byte b = null;
			while(it.hasNext()) {
				i = it.next();
				b = positionTable.get(i)[1];
				if (b != null) // If it is null, it means that the value in the file is still original value and has not been over written
					returnTable.put(i, b); // add to return table the position and original value
			}
		}
		
		return returnTable;
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
	public synchronized SortedMap<Integer, Byte> setWriteDataPositionBuffer(SortedMap<Integer, Byte> s, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		SortedMap<Integer, Byte> sReturn = Collections.synchronizedSortedMap(new TreeMap<Integer, Byte>());;
		
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		// Position table; hashtable of positions and values associated with multiple sessions of this dataset
		SortedMap<Integer, Byte[]> positionTable = TestDimDataWriterTree.LOCK_TABLE.get(this._datasetName);
		if (positionTable == null) {
			positionTable = Collections.synchronizedSortedMap(new TreeMap<Integer, Byte[]>());
			TestDimDataWriterTree.LOCK_TABLE.put(this._datasetName, positionTable);
		}
		
		// set the position table instance variable which can be referenced later
		this._positionTable = positionTable;
		
		// LOCK
		// run through the record id positions and add them to the lock table
		// also determine the low and high ranges so that the precise mapping range can be established
		boolean writeStatus = true;
		String message = null;
		int i = -1;
		byte b;
		
		this._lowRange = s.firstKey();
		this._highRange = s.lastKey();
		
		System.out.println("The first key in the input values = " + this._lowRange);
		System.out.println("The last key in the input values = " + this._highRange);
		
		Iterator<Integer> it = s.keySet().iterator();
		while(it.hasNext()) {
			// get the key and validate it
			i = it.next();
			if (i < 0 || i > this._recordCount - 1) {
				writeStatus = false;
				message = "Positions to be set should be >=0 and <= file length/record count";
				break;
			}
			
			// get the value and validate it
			b = s.get(i);
			if (b < -128 || b > 127) {
				writeStatus = false;
				message = "Values to be set should be >= 0 and <= 255; 0 is reserved to mean no change";
				break;
			}
			
			// Add the record ids to the position table if the record id is not locked
			// Array 0 - new value, array 1 = old value
			if (!positionTable.containsKey(i)) {
				// lock the position by adding it to the position table
				this._positionTable.put(i, new Byte[] {b, null});
				
				// add the position and value to the pos values table; this will be used to 
				// actually write to the file
				this._posValuesTable.put(i, b);
			}
			else {
				if (!this._allowPartial && !this._retryFlag) {
					s.put(i, b);
					writeStatus = false;
					message = "Records locked for updating by another session";
					break;
				}
							
				// if retry flag is true, add the positions to the retry table
				if (this._retryFlag)
					this._retryTable.put(i, sReturn.remove(i)); 
			}
		}
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus) {
			this.removeKeysFromTable();
			throw new Exception (message);
		}
		
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to store keys and values in memory : " + elapsedTimeInMillis + "  Milliseconds");
        
		return sReturn;
		
	}
	
	/**
	 * Method to revove the keys associated with the current session from the position table
	 */
	private void removeKeysFromTable() {
		
		long beginTime = System.nanoTime();
		
		// do we have a valid position values table
		if (this._posValuesTable == null || this._posValuesTable.size() <= 0) return;
		
		// do we have a valid position table
		if (this._positionTable == null)
			this._positionTable = LOCK_TABLE.get(this._datasetName);
		
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
	 * Create a segment with the record size. Each byte in the segment is set to null value
	 * to begin with. Null is ASCII code zero. This is a synchronized method. It ensures that 
	 * no two threads create the same segment
	 * 
	 * @throws Exception
	 */
	private synchronized void createSegment() throws Exception {
		this._status = TestDimDataWriterTree.ACTIVE;
		byte buf = 0;
		
		long beginTime = System.nanoTime();
		
		try {
			File f = new File(this._datasetName);
			if (f.exists())
				throw new Exception("File " + this._datasetName + " exists! Cannot create segment");
			
			RandomAccessFile randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			FileChannel rwChannel = randomAccessFile.getChannel();
			MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, this._recordCount);
			
			for (int i = 0; i < this._recordCount; i++)
			{
				buffer.put(buf);
			}
			
			buffer.force();
			rwChannel.close();
			randomAccessFile.close();
			
			System.out.println("File " + this._datasetName + " length = " + f.length());
			
		}
		catch(Exception e) {
			this._status = TestDimDataWriterTree.FAILED;
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == TestDimDataWriterTree.ACTIVE)
				this._status = TestDimDataWriterTree.COMPLETE;
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
	 * @param writeNew Flag that determines whether we are writing new values or old values 
	 * associated with a rollback
	 * 
	 * @throws Exception
	 */
	 
	public void writeToSegment(boolean autoCommit) throws Exception {
		
		if (this._posValuesTable == null || this._posValuesTable.size() <= 0)
			throw new Exception ("No data specified to write!");
		
		this._status = TestDimDataWriterTree.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception ("File : " + this._datasetName + " does not exist!");
			
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			this._rwChannel = _randomAccessFile.getChannel();
			
			this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_WRITE, this._lowRange, (this._highRange - this._lowRange + 1));
			
			// do we have a valid position table
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._datasetName);
			
			Iterator<Integer> it = this._posValuesTable.keySet().iterator();
			int i = -1;
			
			while(it.hasNext()) {
				i = it.next();
				// add to position table the original value from dataset for the position we are about to write into
				this._positionTable.get(i)[1] = this._buffer.get(i - this._lowRange);
				
				// write to dataset
				this._buffer.put((i - this._lowRange), this._positionTable.get(i)[0]);
				
			}
			
			// check for the retry table.
			if (this._retryTable.size() > 0) {
				this.retry();
			}
			
			// if we are here and if the autocommit flag is set, we can commit
			if (autoCommit)
				this.commit();
			
		}
		catch(Exception e) {
			this._status = TestDimDataWriterTree.FAILED;
			
			// if auto commit flag is set, then rollback automatically
			if (autoCommit)
				this.rollback();
			
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == TestDimDataWriterTree.ACTIVE)
				this._status = TestDimDataWriterTree.COMPLETE;
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("File based read/write time : " + elapsedTimeInMillis + "  Milliseconds");
			
		}
	}
	
	/**
	 * Method to retry writing values for positions in the retry table.
	 */
	private synchronized void retry() throws Exception {
		if (this._retryTable == null || this._retryTable.size() <= 0) return;
		
		if (this._positionTable == null)
			this._positionTable = LOCK_TABLE.get(this._datasetName);
		
		// we will keep retrying till the timeout is reached
		int iterations = HASIDSConstants.RETRY_LIMIT_MILLIS/HASIDSConstants.RETRY_INCREMENT_MILLIS;
		
		// loop through the number of iterations
		for (int j = 0; j < iterations; j++) {
			byte b;
			Integer i = null;
			
			// get the list of positions in the retry table
			Enumeration<Integer> e = this._retryTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
				if (!this._positionTable.containsKey(i)) { // this position is not locked
					// get the byte value
					b = this._retryTable.get(i);
					
					// lock the position 
					this._positionTable.put(i, new Byte[] {b, this._buffer.get((i - this._lowRange))});
					
					// write to the position in the memory buffer
					this._buffer.put((i - this._lowRange), b);
					
					// add it to the pos values table for future commit/rollback
					this._posValuesTable.put(i, this._retryTable.remove(i));
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
		if (this._posValuesTable != null && this._posValuesTable.size() > 0) {
			// rewrite the old data back
			Iterator<Integer> it = this._posValuesTable.keySet().iterator();
			int i = -1;
			Byte b = null;
		
			while(it.hasNext()) {
				i = it.next();
		
				// get the original byte value
				b = this._positionTable.get(i)[1];
			
				// if the byte value is null, it means that this position was never written in the file
				if (b != null)
					this._buffer.put((i - this._lowRange), b);
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
		
		// close all buffers
		_buffer.force();
		_rwChannel.close();
		_randomAccessFile.close();
		
		// set them back to null
		_buffer = null;
		_rwChannel = null;
		_randomAccessFile = null;
		
		// remove the keys
		this.removeKeysFromTable();
	}
	
	
	public void run() {
		// TODO Auto-generated method stub
		try {
			this.writeToSegment(true);
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int fileSize = 2000000000;
		try {
			TestDimDataWriterTree t = new TestDimDataWriterTree ("c:\\users\\dpras\\tempdata\\test_3.DM",fileSize);
			TreeMap<Integer, Byte> tm = new TreeMap<Integer, Byte>();
			
			for (int i = 0; i < 5000000; i++)
				tm.put(i, (byte)((1 + i)%254));
			
			SortedMap<Integer, Byte> s = t.setWriteDataPositionBuffer(tm, false, false);
			System.out.println("No of entries remaining after setting data writer = " + s.size());
			t.writeToSegment(true);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
