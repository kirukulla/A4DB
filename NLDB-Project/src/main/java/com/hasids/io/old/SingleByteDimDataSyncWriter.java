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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;

import java.nio.*;

/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 *
 * This class is a test writer component in NON-BATCH mode i.e. in normal operating mode. 
 * File sizes are restricted to 2 billion bytes.
 * 
 * This is the normal operating mode for all inserts, updates, deletes and selects. Data
 * is synchronized between the writers and readers to ensure that concurrent operations
 * follow proper thread synchronization with resolution of I/O.
 * 
 * The atomizer will be executing in the NON-BATCH mode during normal operation hours and
 * will swith to the BATCH mode during nightly bath operations.
 */
public class SingleByteDimDataSyncWriter implements Runnable {
	
	/*lock - begin transaction, update/write, retry success, update/write, unlock - end transaction, commit*/
	/*lock - begin transaction, update/write, retry failure, rollback - update/write original, unlock - end transaction*/
	
	// Data write buffer that is static
	private static Hashtable<String, Hashtable<Integer, Byte[]>> LOCK_TABLE = 
			new Hashtable<String, Hashtable<Integer, Byte[]>>();
	
	// The position table associated with a dataset stored within the LOCK_TABLE
	Hashtable<Integer, Byte[]> _positionTable = null;
	
	// The position and values table for setting data values
	Hashtable<Integer, Byte> _posValuesTable = null;
	
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
	private int _status = SingleByteDimDataSyncWriter.INACTIVE;

	
	/**
	 * Constructor to use when writing into an existing file
	 * 
	 * @param datasetName Name of the dataset to write data into
	 * 
	 * @throws Exception when the file is not present
	 */
	public SingleByteDimDataSyncWriter(String datasetName) throws Exception {
		// TODO Auto-generated constructor stub
		this._datasetName = datasetName;
		try {
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception("File " + this._datasetName + " does not exist! Open constructor with record length to create a new file");
			
			this._recordCount = ((int)f.length()) - CheckSum.FILE_CHECKSUM_LENGTH;
			System.out.println("File record count : " + this._recordCount);
			
			// generate the session Id for this instance of the write class
			this._sessionId = System.nanoTime();
			
		}
		catch(Exception e) {
			throw new Exception(e.getMessage());
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
	public static Hashtable<Integer, Byte> getLockedKeys(String datasetName) {
		// create the return table
		Hashtable<Integer, Byte> returnTable = new Hashtable<Integer, Byte>();
		
		// get the position table for the input dataset name
		Hashtable<Integer, Byte[]> positionTable = SingleByteDimDataSyncWriter.LOCK_TABLE.get(datasetName);
		if (positionTable != null) { // if we have a position table in memory
			int i;
			Byte b = null;
			
			Enumeration<Integer> e = positionTable.keys();
			while(e.hasMoreElements()) {
				i = e.nextElement();
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
	 * @param position An array of ids whose values must be set and are in ascending order
	 * @param values An array of values associated with the positions
	 * 
	 * @throws Exception
	 */
	public synchronized Hashtable<Integer, Byte> setWriteDataPositionBuffer(int[] position, byte[] values, boolean allowPartial, boolean retryFlag) throws Exception {
		
		long beginTime = System.nanoTime();
		Hashtable<Integer, Byte> sReturn = new Hashtable<Integer, Byte>();

		// LOCK
		// run through the record id positions and add them to the lock table
		// also determine the low and high ranges so that the precise mapping range can be established
		boolean writeStatus = true;
		String message = null;
		int i = -1;

		// loop through to validate if the positions and values are proper and also determine
		// the low and high ranges
		for (i = 0; i < position.length; i++)
		{	
			if (position[i] < 0 || position[i] > this._recordCount - 1) {
				writeStatus = false;
				message = "Positions to be set should be >=0 and <= file length/record count";
				break;
			}
			
			if (values[i] < -128 || values[i] > 127) {
				writeStatus = false;
				message = "Values to be set should be >= -128 and <= 127; 0 is reserved to mean no change";
				break;
			}
			
			if (this._lowRange > position[i])
				this._lowRange = position[i];
			
			if (this._highRange < position[i])
				this._highRange = position[i];		
		}		
						
        System.out.println("The first key in the input values = " + this._lowRange);
		System.out.println("The last key in the input values = " + this._highRange);
		System.out.println("No of values in position buffer : " + position.length);
        
		long endTime = System.nanoTime();
		long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
		System.out.println("Time to determine high and low ranges : " + elapsedTimeInMillis + "  Milliseconds");
		beginTime = System.nanoTime();
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus) {
			throw new Exception (message);
		}
                
		// Set the flags
		this._allowPartial = allowPartial;
		this._retryFlag = retryFlag;
		
		// Position table; hashtable of positions and values associated with multiple sessions of this dataset
		this._positionTable = SingleByteDimDataSyncWriter.LOCK_TABLE.get(this._datasetName);
		if (this._positionTable == null) {
			this._positionTable = new Hashtable<Integer, Byte[]>(position.length);
			SingleByteDimDataSyncWriter.LOCK_TABLE.put(this._datasetName, this._positionTable);
		}
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate position table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
		
		// if writestatus, then we continue with the write, else we stop processing
		if (!writeStatus) {
			throw new Exception (message);
		}
		
		// create the position values hashtable
		this._posValuesTable = new Hashtable<Integer, Byte>(position.length);
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to allocate posvalues table in memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		this._randomAccessFile = new RandomAccessFile(this._datasetName, "r");
		this._rwChannel = _randomAccessFile.getChannel();
		this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_ONLY, (this._lowRange + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1));
		System.out.println("Length of Buffer : " + this._buffer.limit());
		this._buffer.clear();
		
		endTime = System.nanoTime();
		elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to Map file into memory : " + elapsedTimeInMillis + "  Milliseconds");
        beginTime = System.nanoTime();
        
		for (i = 0; i < position.length; i++)
		{
			//if (i % 100000 == 0)
			//	System.out.println("Position : " + position[i]);
			
			// Add the record ids to the position table if the record id is not locked
			// Array 0 - new value, array 1 = old value
			if (!this._positionTable.containsKey(position[i])) {
				//this._buffer.position(position[i]);
				// lock the position by adding it to the position table
				this._positionTable.put(position[i], new Byte[] {values[i], this._buffer.get((position[i] - this._lowRange))});
				
				// add the position and value to the pos values table; this will be used to 
				// actually write to the file
				this._posValuesTable.put(position[i], values[i]);
			}
			else {
				if (!this._allowPartial && !this._retryFlag) {
					sReturn.put(i, values[i]);
					writeStatus = false;
					message = "Records locked for updating by another session";
					break;
				}
							
				// if retry flag is true, add the positions to the retry table
				if (this._retryFlag)
					this._retryTable.put(i, values[i]); 
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
		
		this._status = SingleByteDimDataSyncWriter.ACTIVE;
		
		long beginTime = System.nanoTime();
		
		try {
			
			File f = new File(this._datasetName);
			if (!f.exists())
				throw new Exception ("File : " + this._datasetName + " does not exist!");
			
			this._randomAccessFile = new RandomAccessFile(this._datasetName, "rw");
			this._rwChannel = _randomAccessFile.getChannel();
			
			this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_WRITE, (this._lowRange + CheckSum.FILE_CHECKSUM_LENGTH), (this._highRange - this._lowRange + 1));
			//this._buffer = this._rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, this._recordCount);
			
			// do we have a valid position table
			if (this._positionTable == null)
				this._positionTable = LOCK_TABLE.get(this._datasetName);
			
			int i = -1;
			Enumeration<Integer> e = this._posValuesTable.keys();
			while (e.hasMoreElements()) {
				i = e.nextElement();
				
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
			this._status = SingleByteDimDataSyncWriter.FAILED;
			
			// if auto commit flag is set, then rollback automatically
			if (autoCommit)
				this.rollback();
			
			throw new Exception(e.getMessage());
		}
		finally {
			if (this._status == SingleByteDimDataSyncWriter.ACTIVE)
				this._status = SingleByteDimDataSyncWriter.COMPLETE;
			
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
					this._positionTable.put(i, new Byte[] {b, this._buffer.get(i - this._lowRange)});
					
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
		System.out.println("Rollback, posValuesTable size : " + this._posValuesTable.size());
		if (this._posValuesTable != null && this._posValuesTable.size() > 0) {
			// rewrite the old data back
			Iterator<Integer> it = this._posValuesTable.keySet().iterator();
			
			int i = -1;
			Byte b = null;
		
			while(it.hasNext()) {
				i = it.next();
				System.out.println("Rollback position : " + i);
		
				// get the original byte value
				b = this._positionTable.get(i)[1];
			
				// if the byte value is null, it means that this position was never written in the file
				if (b != null)
					this._buffer.put((i - this._lowRange), b.byteValue());
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
	public synchronized void commit() throws Exception {

		// Write the TS
		long lastModifiedTime = this.writeCSTS();
		
		// close all buffers
		_buffer.force();
		_rwChannel.close();
		_randomAccessFile.close();
		
		// set them back to null
		_buffer = null;
		_rwChannel = null;
		_randomAccessFile = null;
		
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
			this.writeToSegment(true);
		}
		catch (Exception e) {
			;// log this
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int setSize = 1000000;
		String filename = "c:\\users\\dpras\\tempdata\\test_3.DM";
		try {
			SingleByteDimDataSyncWriter t = new SingleByteDimDataSyncWriter (filename);
			
			long beginTime = System.nanoTime();
			int[] positions = new int[setSize]; //position
			byte[] values = new byte[setSize]; // values
			
			for (int i = 0; i < setSize; i++) {
				positions[i] = i;
				values[i] = 65;
			}
			
			long endTime = System.nanoTime();
			long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	        System.out.println("Data preperation time : " + elapsedTimeInMillis + "  Milliseconds");
			
			Hashtable<Integer, Byte> h = t.setWriteDataPositionBuffer(positions, values, false, false);
			System.out.println("No of entries remaining after setting data writer = " + h.size());
			t.writeToSegment(true);
			
			int fileval = CheckSum.computeDS(filename);
			System.out.println(fileval);
			
			long lmt = CheckSum.computeTFS(beginTime);
			System.out.println(beginTime + " / " + lmt);
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
