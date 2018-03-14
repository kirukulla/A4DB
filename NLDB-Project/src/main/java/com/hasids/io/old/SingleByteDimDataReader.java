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

package com.hasids.io.old;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Observable;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;


public class SingleByteDimDataReader extends Observable implements Runnable {

	public static final int TYPE_SET = 1;
	public static final int TYPE_MAP = 2;
	
	private String _datasetName;
	private int _encoding;
	private int _dataLength;
	
	private RoaringBitmap _tempBitset = new RoaringBitmap();
	private BitSet _computedBitSet = null;
	private long _elapsedTimeInMillis = 0L; 
	private int _filteredCount = 0;
	private int _filterLowRange = 1; // for beginning of file, it must be set to 1
	private int _filterHighRange = 0; // high range - exclusive
	
	private byte[] _filter;
	private byte[] _memoryBuffer;
	private boolean _multithread = false;
	
	/**
	 * Default no arguments Constructor 
	 * 
	 * 
	 */
	public SingleByteDimDataReader() {
		super();
	}
	
	/**
	 * Constructor 
	 * 
	 * @param datasetName  
	 * @throws Exception
	 */
	public SingleByteDimDataReader(String dbName, String datasetName) throws Exception {
		this(dbName, datasetName, 1);
	}
	
	/**
	 * Constructor
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @throws Exception
	 */
	public SingleByteDimDataReader(String dbName, String datasetName, int lowRange) throws Exception {
		this (dbName, datasetName, lowRange, HASIDSConstants.DIM_MAX_RECORDS);
	}
	
	/**
	 * Constructor
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @param highRange
	 * @throws Exception
	 */
	public SingleByteDimDataReader(String dbName, String datasetName, int lowRange, int highRange ) throws Exception {
		File f = new File(datasetName);
		if (!f.exists())
			throw new Exception ("File " + datasetName + " does not exist!");
		
		long fileLength = f.length();
		if (fileLength > HASIDSConstants.DIM_MAX_RECORDS + CheckSum.FILE_CHECKSUM_LENGTH)
			throw new Exception("Size of Dimension files cannot exceed " + HASIDSConstants.DIM_MAX_RECORDS + " in the HASIDS System!");
		this._datasetName = datasetName;
		if (lowRange <= 0 || highRange <= 0)
			throw new Exception("Low range must be > 0 and high range must be > 0");
		if (lowRange > highRange)
			throw new Exception("Low range must be <= high range");
		if (lowRange > HASIDSConstants.DIM_MAX_RECORDS || highRange > HASIDSConstants.DIM_MAX_RECORDS)
			throw new Exception("Low range and high range must be <= " + HASIDSConstants.DIM_MAX_RECORDS);
		
		this._filterLowRange = lowRange;
		if (highRange > fileLength)
			this._filterHighRange = (int) fileLength - CheckSum.FILE_CHECKSUM_LENGTH;
		else
			this._filterHighRange = highRange;
		
		int[] fileType = new int[1];
		int[] encoding = new int[1];
		int[] datasize = new int[1];
		short[] decimals = new short[1];
		int[] segmentNo = new int[1];
		
		CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
		
		if (encoding[0] < CheckSum.DIM_ENCODE_TYPE1 || encoding[0] > CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Invalid encoding type in header, Dimension datasets data length must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		System.out.println("Low range : " + this._filterLowRange);
		System.out.println("High range : " + this._filterHighRange);
		this._dataLength = datasize[0];
		this._encoding = encoding[0];
	}
	
	
	public int getLowRange() {
		return this._filterLowRange;
	}
	
	/**
	 * Set an array of characters as filters for data read operations.
	 * 
	 * @param c Array of characters
	 */
	public void setFilter(byte[] c) {
		this._filter = c;
	}
	
	/**
	 * Returns the filter set for the class instance
	 * 
	 * @return filter
	 */
	public byte[] getFilter() {
		return this._filter;
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
	 * Returns the count of keys read from the file and associated to the input filter
	 * 
	 * @return filter count
	 */
	public long getFilteredCount() {
		return this._filteredCount;
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
	 * Retruns the BitSet associated to the filtered keys
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
	
	public RoaringBitmap getResultSet() {
		return this._tempBitset;
	}
	
	public void setComputedBitSet(BitSet b) throws Exception {
		if (b == null)
			throw new Exception ("Invalid BitSet!");
		
		//System.out.println(this._datasetName + " : Received Bitset of size : " + b.length());
		this._computedBitSet = b;
		this._multithread = true;
	}
	
	/**
	 * Read each and every character of the specified MappedByteBuffer that is not a null and set
	 * the bit in the BitSet to 1.
	 * 
	 * @param buffer Mapped Byte Buffer created from a File Channel associated with a
	 * Random Access File.
	 */
	private void readDataAll(MappedByteBuffer buffer) {
		//System.out.println("Checking entire range of characters");
		int read;
    	for (int i = 0; i < buffer.limit(); i++) {
    		
    		// read each character byte
    		read = buffer.get();
    		//_memoryBuffer[i] = (byte)read;
    		
    		// ignore the nulls
			if (read > 0)
				_computedBitSet.set(i);
    	}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (h.get(key) > 0)
    			_computedBitSet.set(key);
    		else
    			_computedBitSet.set(key, false);
    	}
	}
	
	/**
	 * Read each and every character of the specified MappedByteBuffer that is not a null and set
	 * the bit in the BitSet to 1.
	 * 
	 * @param buffer Mapped Byte Buffer created from a File Channel associated with a
	 * Random Access File.
	 */
	private void readDataNull(MappedByteBuffer buffer) {
		//System.out.println("Checking entire range of characters");
		int read;
    	for (int i = 0; i < buffer.limit(); i++) {
    		
    		// read each character byte
    		read = buffer.get();
    		//_memoryBuffer[i] = (byte)read;
    		
    		// ignore the nulls
			if (read == 0)
				_computedBitSet.set(i);
    	}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (h.get(key) == 0)
    			_computedBitSet.set(key);
    		else
    			_computedBitSet.set(key, false);
    	}
	}
	
	/**
	 * 
	 * @param buffer
	 * @param k
	 * @param rangeCheck
	 */
	private void readDataRange(MappedByteBuffer buffer, int k, int[][] rangeCheck) {
		//System.out.println("Checking ranges of characters : " + k);
		int read;
    	for (int i = 0; i < buffer.limit(); i++) {
			
        	// read each character byte
			read = buffer.get();
			//_memoryBuffer[i] = (byte)read;
			// 
			for (int j = 0; j <= k; j++) {
				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) {
					_computedBitSet.set(i);
				}
			}
			
		}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		read = h.get(key);
    		for (int j = 0; j <= k; j++) {
				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) {
					_computedBitSet.set(key);
				}
				else
					_computedBitSet.set(key, false);
			}
    		
    	}
	}
	
	private void readDataFilter(MappedByteBuffer buffer) {
		//System.out.println("Checking ranges of characters : " + this._filter.length);
    	int read;
		for (int i = 0; i < buffer.limit(); i++) {
			
        	// read each character byte
			read = buffer.get();
			//_memoryBuffer[i] = (byte)read;
			for (int j = 0; j < this._filter.length; j++)
				if (read == this._filter[j]) {
					_tempBitset.add(i);
					break;
				}
		}
		
		// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		read = h.get(key);
    		for (int j = 0; j < this._filter.length; j++)
				if (read == this._filter[j])
					_computedBitSet.set(key);
				else
					_computedBitSet.set(key, false);
    	}
	}
	
	private void readDataSingleCheck(MappedByteBuffer buffer) {
		//System.out.println("Checking single character");
		byte read;
    	for (int i = 0; i < buffer.limit(); i++) {
    		
    		// read each character byte
    		read = buffer.get();
    		//_memoryBuffer[i] = (byte)read;
    		// ignore the nulls
			if (read == _filter[0])
				_computedBitSet.set(i);
    	}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (h.get(key) == _filter[0])
    			_computedBitSet.set(key);
    		else
    			_computedBitSet.set(key, false);
    	}
	}
	
	private void readDataAllMulti(MappedByteBuffer buffer) {
		//System.out.println("Checking entire range of characters in multi thread mode");
		int read;
    	for (int i = 0; i < buffer.limit(); i++) {
    		
    		// read each character byte
    		read = buffer.get();
    		
    		// Code for storing the data in memory that can be used later for search
    		//_memoryBuffer[i] = (byte)read;
    		
    		// ignore the nulls
			if (read > 0)
				_computedBitSet.set((int)(i + this._filterLowRange -1));
    	}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > 0)
    			_computedBitSet.set(key + this._filterLowRange -1);
    		else
    			_computedBitSet.set(key + this._filterLowRange -1, false);
    	}
	}
	
	private void readDataRangeMulti(MappedByteBuffer buffer, int k, int[][] rangeCheck) {
		//System.out.println("Checking ranges of characters in multi thread mode : " + k);
		int read;
    	for (int i = 0; i < buffer.limit(); i++) {
			
        	// read each character byte
			read = buffer.get();
			
			// Code for storing the data in memory that can be used later for search
			//_memoryBuffer[i] = (byte)read;
			// 
			for (int j = 0; j <= k; j++) {
				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
					_computedBitSet.set((int)(i + this._filterLowRange -1));
				}
			}
			
		}
    	
    	// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
    			read = h.get(key);
    			for (int j = 0; j <= k; j++) {
    				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) {
    					_computedBitSet.set(key + this._filterLowRange -1);
    				}
    				else
    					_computedBitSet.set(key + this._filterLowRange -1, false);
    			}
    		}
    	}
	}
	
	private void readDataFilterMulti(MappedByteBuffer buffer) {
		//System.out.println("Checking ranges of characters in multi thread mode: " + this._filter.length);
    	int read;
		for (int i = 0; i < buffer.limit(); i++) {
			
        	// read each character byte
			read = buffer.get();
			
			// Code for storing the data in memory that can be used later for search
			//_memoryBuffer[i] = (byte)read;
			for (int j = 0; j < this._filter.length; j++)
				if (read == this._filter[j])
					_computedBitSet.set((int)(i + this._filterLowRange -1));
		}
		
		// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
    			read = h.get(key);
    			for (int j = 0; j < this._filter.length; j++)
    				if (read == this._filter[j])
    					_computedBitSet.set(key + this._filterLowRange -1);
    				else
    					_computedBitSet.set(key + this._filterLowRange -1, false);
    		}
    	}
	}
	
	private void readDataSingleCheckMulti(MappedByteBuffer buffer) {
		//System.out.println("Checking single character in multi thread mode");
		int read;
		for (int i = 0; i < buffer.limit(); i++) {
			
    		// read each character byte
    		read = buffer.get();
    		
    		// Code for storing the data in memory that can be used later for search
    		//_memoryBuffer[i] = (byte)read;
    		// ignore the nulls
			if (read == _filter[0])
				_computedBitSet.set((int)(i + this._filterLowRange -1));
			
    	}
		
		// get any locked original records
    	Hashtable<Integer, Byte> h = SingleByteDimDataSyncWriter.getLockedKeys(this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) == _filter[0])
    			_computedBitSet.set(key + this._filterLowRange -1);
    		else
    			_computedBitSet.set(key + this._filterLowRange -1, false);
    	}
	}
	
	/**
	 * Method to return BitSet for the input filter and range selected from the memory buffer. 
	 * This method is applicable only when the memory buffer is enabled to collect and store the 
	 * reads.
	 * 
	 * @param filter
	 * @param lowRange
	 * @param highRange
	 * @return
	 * @throws Exception
	 */
	private BitSet getDataByFilter(char[] filter, int lowRange, int highRange) throws Exception{
		//System.out.println("Retreiving bitset for range : " + lowRange + "/" + highRange);
    	int read;
    	BitSet b;
    	
    	if (lowRange < 0 || lowRange > highRange || 
    			lowRange >= _memoryBuffer.length || highRange < 0 || 
    			highRange > _memoryBuffer.length)
    		throw new Exception("Invalid low/high range values");
    	
    	b = new BitSet(highRange - lowRange);
		for (int i = 0; i < _memoryBuffer.length; i++) {
			
        	// read each character byte
			read = (char)_memoryBuffer[i];
			
			for (int j = 0; j < this._filter.length; j++)
				if (read == this._filter[j])
					b.set(i);
		}
		
		return b;
	}

	/**
	 * Method to return BitSet for the input filter and range selected from the memory buffer. 
	 * This method is applicable only when the memory buffer is enabled to collect and store the 
	 * reads.
	 * 
	 * @param filter
	 * @param lowRange
	 * @param highRange
	 * @return
	 * @throws Exception
	 */
	private BitSet getDataGTFilter(char filter, int lowRange, int highRange) throws Exception{
		//System.out.println("Retreiving bitset for range : " + lowRange + "/" + highRange);
    	int read;
    	BitSet b;
    	
    	if (lowRange < 0 || lowRange > highRange || 
    			lowRange >= _memoryBuffer.length || highRange < 0 || 
    			highRange > _memoryBuffer.length)
    		throw new Exception("Invalid low/high range values");
    	
    	b = new BitSet(highRange - lowRange);
		for (int i = 0; i < _memoryBuffer.length; i++) {
			
        	// read each character byte
			read = (char)_memoryBuffer[i];
			
			if (read >= filter)
				b.set(i);
		}
		
		return b;
	}
	
	/**
	 * Method to return BitSet for the input filter and range selected from the memory buffer. 
	 * This method is applicable only when the memory buffer is enabled to collect and store the 
	 * reads.
	 * 
	 * @param filter
	 * @param lowRange
	 * @param highRange
	 * @return
	 * @throws Exception
	 */
	private BitSet getDataLTFilter(char filter, int lowRange, int highRange) throws Exception{
		//System.out.println("Retreiving bitset for range : " + lowRange + "/" + highRange);
    	int read;
    	BitSet b;
    	
    	if (lowRange < 0 || lowRange > highRange || 
    			lowRange >= _memoryBuffer.length || highRange < 0 || 
    			highRange > _memoryBuffer.length)
    		throw new Exception("Invalid low/high range values");
    	
    	b = new BitSet(highRange - lowRange);
		for (int i = 0; i < _memoryBuffer.length; i++) {
			
        	// read each character byte
			read = (char)_memoryBuffer[i];
			
			if (read <= filter)
				b.set(i);
		}
		
		return b;
	}
	
	/**
	 * Method to return BitSet for the input filter and range selected from the memory buffer. 
	 * This method is applicable only when the memory buffer is enabled to collect and store the 
	 * reads.
	 * 
	 * @param lowFilter
	 * @param highFilter
	 * @param lowRange
	 * @param highRange
	 * @return
	 * @throws Exception
	 */
	private BitSet getDataInBetweenFilter(char lowFilter, char highFilter, int lowRange, int highRange) throws Exception{
		//System.out.println("Retreiving bitset for range : " + lowRange + "/" + highRange);
    	int read;
    	BitSet b;
    	
    	if (lowRange < 0 || lowRange > highRange || 
    			lowRange >= _memoryBuffer.length || highRange < 0 || 
    			highRange > _memoryBuffer.length)
    		throw new Exception("Invalid low/high range values");
    	
    	b = new BitSet(highRange - lowRange);
		for (int i = 0; i < _memoryBuffer.length; i++) {
			
        	// read each character byte
			read = (char)_memoryBuffer[i];
			
			if (read >= lowFilter && read <= highFilter)
				b.set(i);
		}
		
		return b;
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
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            int mapSize = (this._filterHighRange - this._filterLowRange + 1);
            
            // Read all the bytes other than null if there is no filter
            boolean all = true;
            if (_filter != null && _filter.length > 0)
            	all = false;
            
            boolean singleCheck = false;
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (_filter.length == 1) { // only one item
            	singleCheck = true;
            }
            
            
            // take the filter inputs and divide them up into ranges for efficiency
            int startPoint = -1;
            int endPoint = -1;
            int k = 0;
            int[][] rangeCheck = new int[255][255]; 
            TreeSet<Integer> t = new TreeSet<Integer>();
            if (_filter.length > 1) {
            	// re-orient the filter to be sorted
            	for (int i = 0; i < _filter.length; i++) {
            		t.add((int)_filter[i]);
            	}
            	
            	// read from the sortedset in ascending order
            	Iterator<Integer> it = t.iterator();
            	int temp = -1, prevtemp = -1;
            	k = 0;
            	while(it.hasNext()) {
            		temp = it.next();
            		if (startPoint == -1) {
            			startPoint = temp;
            			rangeCheck[k][0] = startPoint;
            		}
            		
            		if (endPoint == -1) {
            			endPoint = temp;
            			rangeCheck[k][1] = endPoint;
            		}
            		else {
            			if (temp == (prevtemp + 1)) {
            				endPoint = temp;
            				rangeCheck[k][1] = endPoint;
            			}
            			else {
            				++k;
            				startPoint = temp;
            				endPoint = temp;
            				rangeCheck[k][0] = temp;
            				rangeCheck[k][1] = temp;
            			}
            		}
            		prevtemp = temp;
            	}
            }
            
            // once the ranges are established, determine if they are contiguous
            // or fragmented. If they are fragmented, it is better to loop through individual
            // filter checks instead of range checks
            int countFragmented = 0;
            for (int i = 0; i <= k; i++) {
            	if (rangeCheck[i][0] == rangeCheck[i][0])
            		++countFragmented;
            }
            
            boolean checkRange = true;
            if ((k+1)/2 < countFragmented)
            	checkRange = false;
            	
            //System.out.println("No of ranges : " + (k+1));
            
            //System.out.println("Map size/File size in bytes : " + mapSize + "/" + fileSize);
            
            // Temporary buffer to pull data from memory 
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, this._filterLowRange - 1 + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // check if the bitset was set from outside
            if (_computedBitSet == null)
            	_computedBitSet = new BitSet(mapSize);
            	
            			
            // Enable this if we want to store the data in memory as in
            // storing the data in cache for reuse later for doing filtered
            // reads. When this is enabled, ensure the population of
            // the memory buffer is enabled in all the read methods.
            //_memoryBuffer = new byte[(int)fileSize];
            
	        // execute different type of reads based on 
            // whether this is a single character filter, all ids set,
            // range based filter or raw array of filters
            // In addition, if this is running in a multi thread mode
            // wherein the resultant bitset has been set from outside
            // execute the multi reads
            if (all) {
	        	if (this._multithread)
	        		this.readDataAllMulti(buffer);
	        	else
	        		this.readDataAll(buffer);
	        }
            
	        // when filter contains only one character to check
	        else if (singleCheck) {
	        	if (this._multithread)
	        		this.readDataSingleCheckMulti(buffer);
	        	else
	        		this.readDataSingleCheck(buffer);
	        }
	        
	        // multi range check
	        else if (checkRange) {
	        	if (this._multithread)
	        		this.readDataRangeMulti(buffer, k, rangeCheck);
	        	else
	        		this.readDataRange(buffer, k, rangeCheck);
	        }
	        
	        // check ind. filter values
	        else {
	        	if (this._multithread)
	        		this.readDataFilterMulti(buffer);
	        	else
	        		this.readDataFilter(buffer);
	        }
            
	        inChannel.close();
            // close the file
            aFile.close();
            
            //buffer.clear();
            buffer = null;
            
            if (_memoryBuffer != null)
            	System.out.println("Memory Buffer length : " + _memoryBuffer.length);
            
        } catch (IOException ioe) {
            throw new IOException(ioe);
        } finally {
        	
        	// set the record count
        	if (this._computedBitSet != null)
        		this._filteredCount = this._computedBitSet.cardinality();
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
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
		long beginTime = System.nanoTime();
		RoaringBitmap tempBitset = new RoaringBitmap();
		for (int i = 0; i < 250000000; i=i+1) {
			tempBitset.add(i);
		}
		
		long endTime = System.nanoTime();
		long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time for 2 billion inserts into Roaring Bitset : " + diff + " / size : " + tempBitset.getSizeInBytes());
        
        beginTime = System.nanoTime();
        BitSet b = new BitSet(2000000000);
        for (int i = 0; i < 250000000; i=i+8)
        	b.set(i);
        //long[] c = b.toLongArray();
        
        endTime = System.nanoTime();
		diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time for 2 billion flips in Regular Bitset : " + diff);
        
        beginTime = System.nanoTime();
        BitSet bs = new BitSet(2000000000);
        PeekableIntIterator i = tempBitset.getIntIterator();
        while(i.hasNext())
        	bs.set(i.next());
        
        endTime = System.nanoTime();
		diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to convert Roaring to Regular Bitset : " + diff);
        
		/*
		int lowRange = 1;
		int highRange = 100000001;
		
		byte[] c = {5};
		byte[] c1 = {7};
		byte[] c2 = {1};
		byte[] c3 = {2};
		byte[] c4 = {2};
		byte[] c5 = {2};
		
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long diff = 0;
		
		try {
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			SingleByteDimDataReader hr = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			hr.setFilter(c);
			hr.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
			SingleByteDimDataReader hr1 = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr1.getdatasetName());
			hr1.setFilter(c1);
			hr1.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
			SingleByteDimDataReader hr2 = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr2.getdatasetName());
			hr2.setFilter(c2);
			hr2.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
			SingleByteDimDataReader hr3 = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr3.getdatasetName());
			hr3.setFilter(c3);
			hr3.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\race_1.DM";
			SingleByteDimDataReader hr4 = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr4.getdatasetName());
			hr4.setFilter(c4);
			hr4.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
			SingleByteDimDataReader hr5 = new SingleByteDimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr5.getdatasetName());
			hr5.setFilter(c5);
			hr5.readData();
			
			beginTime = System.nanoTime();
			int k = 0;
			
			System.out.println("Begin bit test");
			BitSet result = hr.getResultBitSet();
			BitSet r1 = hr1.getResultBitSet();
			BitSet r2 = hr2.getResultBitSet();
			BitSet r3 = hr3.getResultBitSet();
			BitSet r4 = hr4.getResultBitSet();
			BitSet r5 = hr5.getResultBitSet();
			
			System.out.println("Cardinality 0 : " + result.cardinality());
			System.out.println("Cardinality 1 : " + r1.cardinality());
			System.out.println("Cardinality 2 : " + r2.cardinality());
			System.out.println("Cardinality 3 : " + r3.cardinality());
			System.out.println("Cardinality 4 : " + r4.cardinality());
			System.out.println("Cardinality 5 : " + r5.cardinality());
			
			result.and(r1);
			System.out.println("Intersection 1 Cardinality : " + result.cardinality());
			result.and(r2);
			System.out.println("Intersection 2 Cardinality : " + result.cardinality());
			result.and(r3);
			System.out.println("Intersection 3 Cardinality : " + result.cardinality());
			result.and(r4);
			System.out.println("Intersection 4 Cardinality : " + result.cardinality());
			result.and(r5);
			System.out.println("Intersection 5 Cardinality : " + result.cardinality());
			
			System.out.println("End bit test");
			
			k = result.cardinality();
			System.out.println("No of records matching intersections :" + k);
			
			//long[] l = result.toLongArray();
			//System.out.println("Length of lomg Array : " + l.length);
			
				
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("File read time in millis : " + 
					(hr.getElapsedTime() + hr1.getElapsedTime() + hr2.getElapsedTime() + 
					hr3.getElapsedTime() + hr4.getElapsedTime() + hr5.getElapsedTime()));
			//System.out.println("No of records in result BitSet : " + k);
			System.out.println("Operations time in memory in millis : " + diff);
			
		}
		catch (Exception e ) {
			e.printStackTrace();
		}
		*/
	}


}
