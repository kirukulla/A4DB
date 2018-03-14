/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 * This class is a test dimension data reader wherein the data is stored in the HASIDS/BOBS
 * format. Data is read based on input filters and the same is returned as BitSets. The values
 * in the BitSet set to either 0 or 1, 1 representing the identifiers/cluster ids/unary keys matching
 * the input filer. The class implements both the Runnable and Observable interfaces. Runnable
 * interface implementation allows this class to be executed as a thread within a ThreadGroup. The
 * Observable interface implementation allows this class to be monitored by an Observer.   
 */

package com.hasids.io.uni;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Observable;
import java.util.concurrent.TimeUnit;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;


public class UniDataReader extends Observable implements Runnable {

	//public static final int TYPE_SET = 1;
	//public static final int TYPE_MAP = 2;
	
	private String _datasetName;
	private String _dbName;
	
	private int _encoding;
	private int _dataLength;
	private int _decimals;
	private short _messageTypeSize;
	private int _segmentCount;
	private int _currentRecordCount;
	
	private BitSet _computedBitSet = null;
	private ArrayList<String> _computedList = null;
	private long _elapsedTimeInMillis = 0L; 
	private int _filterLowRange = 1; // for beginning of file, it must be set to 1
	private int _filterHighRange = 0; // high range - exclusive
	
	// parallel processing variables
	private int _startId = 0;
	private int _endId = 0;
	
	/**
	 * Default no arguments Constructor 
	 * 
	 * 
	 */
	public UniDataReader() {
		super();
	}
	
	/**
	 * Constructor 
	 * 
	 * @param datasetName  
	 * @throws Exception
	 */
	public UniDataReader(String dbName, String datasetName) throws Exception {
		this(dbName, datasetName, 1);
	}
	
	/**
	 * Constructor
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @throws Exception
	 */
	public UniDataReader(String dbName, String datasetName, int lowRange) throws Exception {
		this (dbName, datasetName, lowRange, HASIDSConstants.DIM_MAX_RECORDS, 0, 0);
	}
	
	/**
	 * Constructor Default constructor for running in non thread mode
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @param highRange
	 * @throws Exception
	 */
	public UniDataReader(String dbName, String datasetName, int lowRange, int highRange, int startId, int endId ) throws Exception {
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		try {
			
			File f = new File(datasetName);
			if (!f.exists())
				throw new Exception ("File " + datasetName + " does not exist!");
			
			long fileLength = f.length();
			System.out.println("File length of " + datasetName + " = " + fileLength);
			if (fileLength > HASIDSConstants.DIM_MAX_RECORDS + CheckSum.UNIFILE_CHECKSUM_LENGTH)
				throw new Exception("Size of Unary files cannot exceed " + HASIDSConstants.DIM_MAX_RECORDS + " in the HASIDS System!");
			
			this._datasetName = datasetName;
			
			if (lowRange <= 0 || highRange <= 0)
				throw new Exception("Low range must be > 0 and high range must be > 0");
			if (lowRange > highRange)
				throw new Exception("Low range must be <= high range");
			if (lowRange > HASIDSConstants.DIM_MAX_RECORDS || highRange > HASIDSConstants.DIM_MAX_RECORDS)
				throw new Exception("Low range and high range must be <= " + HASIDSConstants.DIM_MAX_RECORDS);
			this._filterLowRange = lowRange;
			
			int[] fileType = new int[1];
			int[] encoding = new int[1];
			int[] segmentNo = new int[1];
			int[] datasize = new int[1];
			short[] decimals = new short[1];
			int[] segmentCount = new int[1];
			
			CheckSum.validateUNIFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo, segmentCount);
			
			if (fileType[0] != CheckSum.FILE_TYPE_UNI)
				throw new Exception("Invalid file type!");
			
			if (encoding[0] != CheckSum.UNI_ENCODE_TYPE_OLAP && encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC)
				throw new Exception ("Invalid encoding type in header for unary datasets ");
			
			if (datasize[0] <= 0)
				throw new Exception ("Invalid datasize in header for unary datasets ");
			
			// get the record count
			this._segmentCount = segmentCount[0];
			System.out.println("Segment count from CheckSum: " + this._segmentCount);
			
			if (this._segmentCount < highRange)
				this._filterHighRange = this._segmentCount;
			else
				this._filterHighRange = highRange;
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 2 ||
					encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 0)
				throw new Exception ("Invalid decimal in header for unary document datasets ");
			
			
			this._dataLength = datasize[0]; // This is hardcoded as unary sets will store only integers
			this._decimals = decimals[0];
			this._encoding = encoding[0];
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC)
				this._messageTypeSize = 2;
			else
				this._messageTypeSize = 0;
			
			this._currentRecordCount = (int)(f.length() - CheckSum.UNIFILE_CHECKSUM_LENGTH)/(this._messageTypeSize + this._dataLength + this._decimals);
			
			// if full file read
			if (startId == 0 && endId == 0) {
				this._startId = 1;
				this._endId = this._currentRecordCount;
			}
			else {
				if (startId < 1 || startId > endId)
					throw new Exception("Starting search range must be > 0 and less than ending search range");
				if (endId > this._currentRecordCount)
					throw new Exception("Ending search range must be <= current record count");
				
				this._startId = startId;
				this._endId = endId;
			}
			
			System.out.println("Start id : " + this._startId);
			System.out.println("End id : " + this._endId);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * Constructor called in parallel mode usually by the wrapper reader
	 * 
	 * @param dbName
	 * @param datasetName
	 * @param lowRange
	 * @param highRange
	 * @param startId
	 * @param endId
	 * @param fileType
	 * @param encoding
	 * @param datasize
	 * @param decimals
	 * @param segmentCount
	 * @throws Exception
	 */
	public UniDataReader(String dbName, String datasetName, int lowRange, int highRange, int startId, int endId, 
			int fileType[], int encoding[], int datasize[], int decimals[], int segmentCount[] ) throws Exception {
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		try {
			
			File f = new File(datasetName);
			if (!f.exists())
				throw new Exception ("File " + datasetName + " does not exist!");
			
			long fileLength = f.length();
			System.out.println("File length of " + datasetName + " = " + fileLength);
			if (fileLength > HASIDSConstants.DIM_MAX_RECORDS + CheckSum.UNIFILE_CHECKSUM_LENGTH)
				throw new Exception("Size of Unary files cannot exceed " + HASIDSConstants.DIM_MAX_RECORDS + " in the HASIDS System!");
			
			this._datasetName = datasetName;
			
			if (lowRange <= 0 || highRange <= 0)
				throw new Exception("Low range must be > 0 and high range must be > 0");
			if (lowRange > highRange)
				throw new Exception("Low range must be <= high range");
			if (lowRange > HASIDSConstants.DIM_MAX_RECORDS || highRange > HASIDSConstants.DIM_MAX_RECORDS)
				throw new Exception("Low range and high range must be <= " + HASIDSConstants.DIM_MAX_RECORDS);
			this._filterLowRange = lowRange;
			
			
			/*int[] fileType = new int[1];
			int[] encoding = new int[1];
			int[] datasize = new int[1];
			int[] decimals = new int[1];
			int[] segmentCount = new int[1];
			
			CheckSum.validateUNIFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentCount);
			*/
			
			if (fileType[0] != CheckSum.FILE_TYPE_UNI)
				throw new Exception("Invalid file type!");
			
			if (encoding[0] != CheckSum.UNI_ENCODE_TYPE_OLAP && encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC)
				throw new Exception ("Invalid encoding type in header for unary datasets ");
			
			if (datasize[0] <= 0)
				throw new Exception ("Invalid datasize in header for unary datasets ");
			
			// get the record count
			this._segmentCount = segmentCount[0];
			System.out.println("Segment count from CheckSum: " + this._segmentCount);
			
			if (this._segmentCount < highRange)
				this._filterHighRange = this._segmentCount;
			else
				this._filterHighRange = highRange;
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 2 ||
					encoding[0] != CheckSum.UNI_ENCODE_TYPE_DOC && decimals[0] != 0)
				throw new Exception ("Invalid decimal in header for unary document datasets ");
			
			
			this._dataLength = datasize[0]; // This is hardcoded as unary sets will store only integers
			this._decimals = decimals[0];
			this._encoding = encoding[0];
			
			if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC)
				this._messageTypeSize = 2;
			else
				this._messageTypeSize = 0;
			
			this._currentRecordCount = (int)(f.length() - CheckSum.UNIFILE_CHECKSUM_LENGTH)/(this._messageTypeSize + this._dataLength + this._decimals);
			
			// if full file read
			if (startId == 0 && endId == 0) {
				this._startId = 1;
				this._endId = this._currentRecordCount;
			}
			else {
				if (startId < 1 || startId > endId)
					throw new Exception("Starting search range must be > 0 and less than ending search range");
				if (endId > this._currentRecordCount)
					throw new Exception("Ending search range must be <= current record count");
				
				this._startId = startId;
				this._endId = endId;
			}
			
			System.out.println("Start id : " + this._startId);
			System.out.println("End id : " + this._endId);
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * Method to set a collection BitSet from external caller to enable parallel processing
	 * 
	 * @param b BitSet being set
	 * @throws Exception
	 */
	public void setResultSets(BitSet b, ArrayList<String> al) throws Exception {
		if (b == null)
			throw new Exception ("Invalid BitSet!");
		
		if (al == null && this._encoding == CheckSum.UNI_ENCODE_TYPE_DOC)
			throw new Exception ("Invalid ArrayList for a document type!");
		
		//System.out.println(this._datasetName + " : Received Bitset of size : " + b.length());
		this._computedBitSet = b;
		this._computedList = al;
	}
	
	/**
	 * Returns the dataset datasetName associated with the class instance.
	 * 
	 * @return dataset name
	 */
	public String getdatasetName() {
		return this._datasetName;
	}

	/**
	 * Returns the dataset datasetName associated with the class instance.
	 * 
	 * @return dataset name
	 */
	public String getdbName() {
		return this._dbName;
	}
	
	/**
	 * returns the time elapsed for the file read operations.
	 * 
	 * @return elapsed time
	 */
	public long getElapsedTime() {
		return this._elapsedTimeInMillis;
	}
	
	/**
	 * Returns the low range of the key set associated to the filter. This need not be the 
	 * segment low value, but a position within the file where the first occurrence of the
	 * filter is found.
	 * 
	 * @return filter low range
	 */
	public long getFilterLowRange() {
		return this._filterLowRange;
	}
	
	/**
	 * Returns the high range of the key set associated to the filter. This need not be the 
	 * segment high value, but a position within the file where the last occurrence of the
	 * filter is found.
	 * 
	 * @return filter low range
	 */
	public long getFilterHighRange() {
		return this._filterHighRange;
	}
	
	/**
	 * Returns the encoding type associated with the dataset. T
	 * 
	 * @return filter Encoding
	 */
	public int getEncoding () {
		return this._encoding;
	}
	
	/**
	 * Retuns the BitSet associated to the filtered keys
	 * 
	 * @return Result Bit Set
	 */
	public BitSet getResultBitSet() {
		// If just querying and the object is going to be thrown away then
		// there is no need to clone. However, if the Object will be stored in cache then
		// return the clone.
		//return this._computedBitSet;
		return (BitSet) this._computedBitSet.clone();
	}
	
	/**
	 * Returns the ArrayList containing matching record ids along with the counts.
	 * 
	 * @return
	 */
	public ArrayList<String> getResultSet() {
		return this._computedList;
	}
	
	/**
	 * Method to read, filter and store the record ids matching the input range
	 *  
	 * @param buffer
	 */
	private void readDataOLAPUnfiltered(MappedByteBuffer buffer) {
		System.out.println("Current method readDataOLAPUnfiltered");
		if (_computedBitSet == null)
        	_computedBitSet = new BitSet(this._segmentCount);
		
		int read = 0;
        for (int i = 0; i < this._currentRecordCount; i++) {
    		
			// read each character byte
			read = buffer.getInt();
    		
			// ignore the nulls
			_computedBitSet.set(read);		
		}
	}
	
	/**
	 * Method to read, filter and store the record ids matching the input range
	 *  
	 * @param buffer
	 */
	private void readDataOLAPFiltered(MappedByteBuffer buffer) {
		if (_computedBitSet == null)
        	_computedBitSet = new BitSet(this._segmentCount);
		
		int read = 0;
        for (int i = 0; i < this._currentRecordCount; i++) {
    		
			// read each character byte
			read = buffer.getInt();
    		
				// ignore the nulls
			if (read >= this._filterLowRange && read <= this._filterHighRange)
				_computedBitSet.set(read);		
		}
	}
	/**
	 * Method to read, filter and store the record ids and corresponding counts that match the input range
	 * 
	 * @param buffer
	 */
	private void readDataDOC(MappedByteBuffer buffer) {
		if (_computedBitSet == null)
        	_computedBitSet = new BitSet(this._segmentCount);
		
		if(this._computedList == null)
			this._computedList = new ArrayList<String>(this._currentRecordCount);
        
        int read = 0;
        short count = 0;
        short messageType = 0;
        for (int i = 0; i < this._currentRecordCount; i++) {
    		// read the messageType
        	messageType = buffer.getShort();
        	
			// read each character byte
			read = buffer.getInt();
    		
				// ignore the nulls
			if (read >= this._filterLowRange && read <= this._filterHighRange) {
				_computedBitSet.set(i);
				count = buffer.getShort();
				this._computedList.add(messageType + "," + i + "," + count);
			}		
		}
			
	}
	
	/**
	 * The main method to read the data from the dimension dataset. before actual read begins
	 * pre-processing is done to arrange the filters in proper order to speed up the checks and
	 * flags set to determine the actual filter method to call.
	 * 
	 * @throws Exception
	 */
	private void readData () throws Exception {
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            //int mapSize = this._currentRecordCount * this._dataLength;
            int mapSize = (this._endId - this._startId + 1) * (this._messageTypeSize + this._dataLength + this._decimals);
            
            System.out.println("Map size : " + mapSize);
            
            // Temporary buffer to pull data from memory 
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 
            		CheckSum.UNIFILE_CHECKSUM_LENGTH + (this._startId - 1) * (this._messageTypeSize + this._dataLength + this._decimals), 
            		mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            // check if the bitset was set from outside
            
            if (this._encoding == CheckSum.UNI_ENCODE_TYPE_DOC)
            	this.readDataDOC(buffer);
            else if (this._encoding == CheckSum.UNI_ENCODE_TYPE_OLAP) {
            	System.out.println("Current Record count / High filter record Id : " + this._currentRecordCount + " / " + this._filterHighRange);
            	
            	if (this._currentRecordCount == this._filterHighRange)
            		this.readDataOLAPUnfiltered(buffer);
            	else
            		this.readDataOLAPFiltered(buffer);
            }
            buffer = null;
            
            inChannel.close();
            // close the file
            aFile.close();
            
        } catch (IOException ioe) {
            throw new IOException(ioe);
        } finally {
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
	}
	
	/**
	 * Method to get data in a non thread mode in BitSet mode
	 */
	public BitSet getDataRecordIds() {
		try {
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	/**
	 * Method to get data in a non thread mode in BitSet mode
	 */
	public ArrayList<String> getDataRecordIdsAndCounts() {
		try {
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultSet();
	}
	
	public void run() {
		
		try {
			// execute the read file method
			this.readData();
		
		}
		catch (Exception ioe) {
			;
		}
		
		// inform observers that the process is complete
		this.notifyObservers(this._datasetName);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		String dbName = "Test";
		String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\samsung_1.UN";
		
		try {
			UniDataReader udr = new UniDataReader(dbName, datasetName);
			BitSet b = udr.getDataRecordIds();
			System.out.println("Result set count : " + b.cardinality());
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


}
