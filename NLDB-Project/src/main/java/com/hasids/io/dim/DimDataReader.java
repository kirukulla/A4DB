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

package com.hasids.io.dim;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Observable;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;


public class DimDataReader extends Observable implements Runnable {

	//public static final int TYPE_SET = 1;
	//public static final int TYPE_MAP = 2;
	
	private String _datasetName;
	private String _dbName;
	
	private int _encoding;
	private int _segmentNo;
	private int _dataLength;
	
	private BitSet _computedBitSet = null;
	private long _elapsedTimeInMillis = 0L; 
	private int _filteredCount = 0;
	private int _filterLowRange = 1; // for beginning of file, it must be set to 1
	private int _filterHighRange = 0; // high range - exclusive
	private boolean _readDirty = false;
	
	private int[] _filter;
	private int _gtFilter;
	private int _ltFilter;
	private int _between1Filter;
	private int _between2Filter;
	
	private boolean _multiIn = false;
	private boolean _singleIn = false;
	private boolean _not = false;
	private boolean _gt = false;
	private boolean _lt = false;
	private boolean _gtEq = false;
	private boolean _ltEq = false;
	private boolean _between = false;
	
	
	private boolean _multithread = false;
	
	private String _classDescription;
	
	/**
	 * Default no arguments Constructor 
	 * 
	 * 
	 */
	public DimDataReader() {
		super();
	}
	
	/**
	 * Constructor 
	 * 
	 * @param datasetName  
	 * @throws Exception
	 */
	public DimDataReader(String dbName, String datasetName) throws Exception {
		this(dbName, datasetName, 1);
	}
	
	/**
	 * Constructor
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @throws Exception
	 */
	public DimDataReader(String dbName, String datasetName, int lowRange) throws Exception {
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
	public DimDataReader(String dbName, String datasetName, int lowRange, int highRange ) throws Exception {
		
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
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
		
		int[] fileType = new int[1];
		int[] encoding = new int[1];
		int[] segmentNo = new int[1];
		int[] datasize = new int[1];
		short[] decimals = new short[1];
		
		CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
		
		if (encoding[0] < CheckSum.DIM_ENCODE_TYPE1 || encoding[0] > CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Invalid encoding type in header, Dimension datasets data length must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		if ((encoding[0] == CheckSum.DIM_ENCODE_TYPE1 && datasize[0] != 1) ||
				(encoding[0] == CheckSum.DIM_ENCODE_TYPE2 && datasize[0] != 2) ||
				(encoding[0] == CheckSum.DIM_ENCODE_TYPE3 && datasize[0] != 4))
			throw new Exception ("Check Sum error, encoding and size do not match");

		if (highRange > (fileLength - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0])
			this._filterHighRange = (int) (fileLength - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
		else
			this._filterHighRange = highRange;
		
		
		this._dataLength = datasize[0];
		this._encoding = encoding[0];
		this._segmentNo = segmentNo[0];
		
		this._classDescription = this.getClass().getName() + "//Database Name: " + this._dbName +
				", Dataset Name : " + this._datasetName + ", Low range : " + this._filterLowRange + 
				", High Range : " + this._filterHighRange;
	}
	
	/**
	 * Method specifically for MPP called by the wrapper
	 * 
	 * @param dbName
	 * @param datasetName
	 * @param lowRange
	 * @param highRange
	 * @param fileType
	 * @param encoding
	 * @param segmentNo
	 * @param dataLength
	 * @param decimals
	 * @throws Exception
	 */
	public DimDataReader(String dbName, String datasetName, int lowRange, int highRange, int fileType, int encoding, int segmentNo, int dataLength, short decimals ) throws Exception {
		
		this._dbName = dbName;
		this._datasetName = datasetName;
		this._filterLowRange = lowRange;
		this._filterHighRange = highRange;
		this._dataLength = dataLength;
		this._encoding = encoding;
		this._segmentNo = segmentNo;
		
		this._classDescription = this.getClass().getName() + "//Database Name: " + this._dbName +
				", Dataset Name : " + this._datasetName + ", Low range : " + this._filterLowRange + 
				", High Range : " + this._filterHighRange;
	}


	
	/**
	 * Method to get a list of all record ids that have a non null value
	 * 
	 * @param buffer MappedByteBuffer from which data will be read
	 * @param multi Flag indicating whether this is a single or multi-threaded read
	 */
	private void readDataAllMulti(MappedByteBuffer buffer, boolean multi) {
		System.out.println("ALL NOT NULL FILTER");
		// offset to current position
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
	    	// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) != 0)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	/**
	 * Method to get the record ids matching the input range
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param k The limit of the range
	 * @param rangeCheck The two array range, depecting the from and to
	 * @param multi Flag indicating if the read operation is single or multi-threaded 
	 */
	private void readDataRangeMulti(MappedByteBuffer buffer, int k, int[][] rangeCheck, boolean multi) {
		//System.out.println("RANGE CHECK FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
    	int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				for (int j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						_computedBitSet.set((int)(i + offset));
						break;
					}
				}
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				for (int j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						_computedBitSet.set((int)(i + offset));
						break;
					}
				}
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				for (int j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						_computedBitSet.set((int)(i + offset));
						break;
					}
				}
			}
		}
		
		if (!this._readDirty) {
	    	// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
	    			read = h.get(key);
	    			for (int j = 0; j <= k; j++) {
	    				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) {
	    					_computedBitSet.set(key + offset);
	    				}
	    				else
	    					_computedBitSet.set(key + offset, false);
	    			}
	    		}
	    	}
		}
	}
	
	private void readDataRangeMultiNot(MappedByteBuffer buffer, int k, int[][] rangeCheck, boolean multi) {
		System.out.println("NOT RANGE CHECK FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
    	int read;
		int count = buffer.limit()/this._dataLength;
		int j = 0;
		boolean found = false;
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
				found = false;
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				for (j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						found = true;
						break;
					}
				}
				
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		found = false;
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				for (j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						found = true;
						break;
					}
				}
				
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		found = false;
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				for (j = 0; j <= k; j++) {
					if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) { // new line character
						found = true;
						break;
					}
				}
				
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
	    	// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
	    			read = h.get(key);
	    			found = false;
	    			for (j = 0; j <= k; j++) {
	    				if (read >= rangeCheck[j][0] && read <= rangeCheck[j][1]) {
	    					_computedBitSet.set(key + offset, false);
	    					found = true;
	    					break;
	    				}
	    			}
	    			if (!found)
	    				_computedBitSet.set(key + offset);
	    		}
	    	}
		}
	}
	
	/**
	 * Method to get the record ids matching the filter set
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating if the read is single or multi-threaded
	 * 
	 */
	private void readDataFilterMulti(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("IN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				for (int j = 0; j < this._filter.length; j++)
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				for (int j = 0; j < this._filter.length; j++)
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				for (int j = 0; j < this._filter.length; j++)
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
	    			read = h.get(key);
	    			for (int j = 0; j < this._filter.length; j++)
	    				if (read == this._filter[j])
	    					_computedBitSet.set(key + offset);
	    				else
	    					_computedBitSet.set(key + offset, false);
	    		}
	    	}
		}
	}
	
	private void readDataFilterMultiNot(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("NOT IN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		int j = 0;
		int k = this._filter.length;
		boolean found = false;
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
				found = false;
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				for (j = 0; j < k; j++) {
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) {
						found = true;
						break;
					}
				}
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
				found = false;
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				for (j = 0; j < k; j++) {
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) {
						found = true;
						break;
					}
				}
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
				found = false;
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				for (j = 0; j < k; j++) {
					//if (read == this._filter[j])
					if ((read ^ this._filter[j]) == 0) { 
						found = true;
						break;
					}
				}
				if (!found)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange) {
	    			read = h.get(key);
	    			found = false;
	    			for (j = 0; j < k; j++) {
	    				if ((read ^ this._filter[j]) == 0) {
	    					_computedBitSet.set(key + offset, false);
	    					found = true;
	    					break;
	    				} 
	    			}
	    			
	    			if (!found)
						_computedBitSet.set((int)(key + offset));
	    		}
	    	}
		}
	}
	
	/**
	 * Method to get the record ids matching the single character filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating if the read is single or multi-threaded
	 * 
	 */
	private void readDataSingleCheckMulti(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("EQUAL FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) == _filter[0])
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	private void readDataSingleCheckMultiNot(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("NOT EQUAL FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				//if (read == _filter[0])
				if ((read ^ this._filter[0]) != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) != _filter[0])
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	/**
	 * Method to get record ids whose value is greater than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGT(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) > _gtFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	private void readDataGTEQ(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= _gtFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) > _gtFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	/**
	 * Method to get record ids whose value is lesser than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataLT(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("LT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange) 
	    			if (h.get(key) < _ltFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}	
	
	private void readDataLTEQ(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("LTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) < _ltFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	/**
	 * Method to get record ids whose value is between the low and high of the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataBETWEEN(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= _between1Filter && read <= _between2Filter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= _between1Filter && read <= _between2Filter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= _between1Filter && read <= _between2Filter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) >= _between1Filter && h.get(key) <= _between2Filter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}	
	
	/**
	 * Method to get record ids whose value is greater than the input filter and lesset than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTOrLT(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GT OR LT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > _gtFilter || read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > _gtFilter || read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > _gtFilter || read < _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) > _gtFilter || h.get(key) < _ltFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	private void readDataGTEQOrLTEQ(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GTEQ OR LTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= _gtFilter || read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= _gtFilter || read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= _gtFilter || read <= _ltFilter)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if (h.get(key) >= _gtFilter || h.get(key) <= _ltFilter)
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	/**
	 * Method to get record ids whose value is greater than the input GT filter, lesser than the input LT filter
	 * and in between the low and high of the input BETWEEN filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTOrLTOrBETWEEN(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GT OR LT OR BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > _gtFilter || read < _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > _gtFilter || read < _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > _gtFilter || read < _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	int value;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		value = h.get(key);
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if ((value > _gtFilter || value < _ltFilter) || 
	    				(value >= _between1Filter && value <= _between2Filter))
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
	    	}
		}
	}
	
	private void readDataGTEQAndLTEQandBETWEEN(MappedByteBuffer buffer, boolean multi) {
		//System.out.println("GTEQ OR LTEQ OR BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1) { // single byte
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= _gtFilter || read <= _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2) { // double byte
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= _gtFilter || read <= _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4) { // four bytes
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= _gtFilter || read <= _ltFilter || (read >= _between1Filter && read <= _between2Filter))
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		if (!this._readDirty) {
			// get any locked original records
	    	Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
	    	Enumeration<Integer> e = h.keys();
	    	int key = -1;
	    	int value;
	    	while (e.hasMoreElements()) {
	    		key = e.nextElement();
	    		value = h.get(key);
	    		if (key >= this._filterLowRange && key <= this._filterHighRange)
	    			if ((value >= _gtFilter || value <= _ltFilter) || 
	    				(value >= _between1Filter && value <= _between2Filter))
	    				_computedBitSet.set(key + offset);
	    			else
	    				_computedBitSet.set(key + offset, false);
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
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            
            // Read all the bytes other than null if there is no filter
            boolean all = true;
            if (this._filter != null && this._filter.length > 0)
            	all = false;
            
            boolean singleCheck = false;
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (this._filter != null && this._filter.length == 1) { // only one item
            	singleCheck = true;
            }
            
            
            // take the filter inputs and divide them up into ranges for efficiency
            int startPoint = -1;
            int endPoint = -1;
            int k = 0;
            int[][] rangeCheck = new int[255][255]; 
            TreeSet<Integer> t = new TreeSet<Integer>();
            if (this._filter != null && this._filter.length > 1) {
            	
            	rangeCheck = new int[this._filter.length][this._filter.length];
            	
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
            	if (rangeCheck[i][0] == rangeCheck[i][1])
            		++countFragmented;
            }
            
            boolean checkRange = true;
            if (countFragmented > 0)
            	checkRange = false;
            
            //System.out.println("No of ranges in file : " + this._datasetName + " : " + (k+1));
            
            //System.out.println("Map size/File size in bytes : " + mapSize + "/" + fileSize);
            
            // Temporary buffer to pull data from memory 
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // check if the bitset was set from outside
            if (_computedBitSet == null)
            	_computedBitSet = new BitSet(this._filterHighRange - this._filterLowRange + 1);
            	
            			
            // execute different type of reads based on 
            // whether this is a single character filter, all ids set,
            // range based filter or raw array of filters
            // In addition, if this is running in a multi thread mode
            // wherein the resultant bitset has been set from outside
            // execute the multi reads
            if (all) {
            	if (this._gt && this._lt && this._between)
            		this.readDataGTOrLTOrBETWEEN(buffer, this._multithread);	
            	else if (this._gtEq && this._ltEq && this._between)
            		this.readDataGTEQAndLTEQandBETWEEN(buffer, this._multithread);	
            	else if(this._gt && this._lt)
            		this.readDataGTOrLT(buffer, this._multithread);
            	else if(this._gtEq && this._ltEq)
            		this.readDataGTEQOrLTEQ(buffer, this._multithread);
            	else if (this._gt)
            		this.readDataGT(buffer, this._multithread);
            	else if (this._gtEq)
            		this.readDataGTEQ(buffer, this._multithread);
            	else if (this._lt)
            		this.readDataLT(buffer, this._multithread);
            	else if (this._ltEq)
            		this.readDataLTEQ(buffer, this._multithread);
            	else if (this._between)
            		this.readDataBETWEEN(buffer, this._multithread);
            	else
            		this.readDataAllMulti(buffer, this._multithread);
	        }
            
	        // when filter contains only one character to check
	        else if (singleCheck) {
	        	if (!this._not)
	        		this.readDataSingleCheckMulti(buffer, this._multithread);
	        	else
	        		this.readDataSingleCheckMultiNot(buffer, this._multithread);
	        }
	        
	        // multi range check
	        else if (checkRange) {
	        	if (!this._not)
	        		this.readDataRangeMulti(buffer, k, rangeCheck, this._multithread);
	        	else
	        		this.readDataRangeMultiNot(buffer, k, rangeCheck, this._multithread);
	        }
	        
	        // check ind. filter values
	        else {
	        	if (!this._not)
	        		this.readDataFilterMulti(buffer, this._multithread);
	        	else
	        		this.readDataFilterMultiNot(buffer, this._multithread);
	        }
            
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
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
	
	/**
	 * Method that returns the values associated with the input positions
	 * 
	 * @param positions An array of positions whose values must be returned, for efficiency ensure
	 * the positions are sorted
	 * @return An array of values matching the input positions
	 * 
	 * @throws Exception
	 */
	private int[] readValues (int[] positions) throws Exception {
		
		int[] values = new int[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            if (this._dataLength == 1) {
            	for (int i = 0; i < positions.length; i++) {
            		values[i] = buffer.get(positions[i] - 1);
            	}
            }
            else if (this._dataLength == 2) {
            	for (int i = 0; i < positions.length; i++) {
            		values[i] = buffer.getShort((positions[i] - 1) * this._dataLength);
            	}
            }
            else if (this._dataLength == 4) {
            	for (int i = 0; i < positions.length; i++) {
            		values[i] = buffer.getInt((positions[i] - 1) * this._dataLength);
            	}
            }
            
            // clear the buffer
            buffer = null;
            // close the hannel
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
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
        
        return values;
        
	}
	
	/**
	 * Returns the filter set for the class instance
	 * 
	 * @return filter
	 */
	public int[] getFilter() {
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
	 * Returns the encoding type associated with the dataset. T
	 * 
	 * @return filter Encoding
	 */
	public int getEncoding () {
		return this._encoding;
	}

	/**
	 * Returns the encoding type associated with the dataset. T
	 * 
	 * @return filter Encoding
	 */
	public int getSegmentNo () {
		return this._segmentNo;
	}
	
	public int getDataLength() {
		return this._dataLength;
	}
	
	public String getClassDescription () {
		return this._classDescription;
	}
	
	public void readDirty() {
		this._readDirty = true;
	}
	
	public void readCommited() {
		this._readDirty = false;
	}
	
	/**
	 * Set an array of characters as filters for data read operations.
	 * 
	 * @param c Array of characters
	 */
	public void setFilter(int[] c) throws Exception {
		if (c == null || c.length <= 0)
			throw new Exception("Null filter received!");
		
		if (c.length > 1) {
			this._singleIn = false;
			this._multiIn = true;
		}
		else {
			this._singleIn = true;
			this._multiIn = false;
		}
		
		this._filter = c;
		this._not = false;
	}
	
	public void setNotFilter(int[] c) throws Exception {
		if (c == null || c.length <= 0)
			throw new Exception("Null filter received!");
		
		if (c.length > 1) {
			this._singleIn = false;
			this._multiIn = true;
		}
		else {
			this._singleIn = true;
			this._multiIn = false;
		}
		
		this._filter = c;
		this._not = true;
	}
	
	/**
	 * Set the character to get all record ids greater than the set character
	 * 
	 * @param c Greater Than character filter 
	 */
	public void setGTFilter(int c) {
		this._gtFilter = c;
		this._gt = true;
	}
	
	public void setGTEQFilter(int c) {
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	/**
	 * Set the character to get all record ids lesser than the set character
	 * 
	 * @param c Lesser Than character filter 
	 */
	public void setLTFilter(int c) {
		this._ltFilter = c;
		this._lt = true;
	}
	
	public void setLTEQFilter(int c) {
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	/**
	 * Set the Greater than and Lesser Than characters, both inclusive to get all record ids 
	 * between the tow characters set
	 * 
	 * @param c1
	 * @param c2
	 */
	public void setBETWEENFilter(int c1, int c2) {
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	/**
	 * Get all characters in the order of the input positions
	 * 
	 * @param positions An array of record ids for whom values must be returned
	 * 
	 * @return An integer array of values for the matching positions in the same order as the
	 * input positions
	 * 
	 * @throws Exception
	 */
	public int[] getDataValues(int[] positions) throws Exception {
		
		if (positions == null || positions.length <= 0)
			throw new Exception ("Input positions cannot be null or empty");
		
		for (int i = 0; i < positions.length; i++)
			if (positions[i] < 1)
				throw new Exception("Positions for getting values cannot be < 1");
		
		return this.readValues(positions);
	}
	
	/**
	 * Returns the BitSet associated to the filtered keys
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
	 * Method to get an ordered array of record ids matching the filter
	 * 
	 * @return ordered array of record ids matching the filter
	 * @throws Exception
	 */
	public int[] getResultSetArray() throws Exception {
		long beginTime = System.nanoTime();
		int[] retVal = null;
		if (this._multithread)
			throw new Exception("This method must be called only when running in a single thread mode, use the calling wrapper for the Array of matching record ids!");
		
		retVal = this._computedBitSet.stream().toArray();
		
		long endTime = System.nanoTime();
		long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to convert from BitSet to Array " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
		return retVal;
	}
	
	/**
	 * Method to set the BitSet into which matching record ids are set
	 * 
	 * @param Bitset into which matching record ids will be set
	 * 
	 * @throws Exception
	 */
	public void setComputedBitSet(BitSet b) throws Exception {
		if (b == null)
			throw new Exception ("Invalid BitSet!");
		
		//System.out.println(this._datasetName + " : Received Bitset of size : " + b.length());
		this._computedBitSet = b;
		this._multithread = true;
	}	
	
	/**
	 * Method to get BOSS result sets for the each unique value, value represented by the location of the
	 * BitSet in the array
	 * 
	 * @param dbName Database Name
	 * @param datasetName Dataset Name
	 * @param noDistributions No of unique values associated with the Dataset Name
	 * @return
	 */
	public static BitSet[] getDataDistributions(String dbName, String datasetName, int noDistributions) {
		BitSet[] b = new BitSet[noDistributions + 1];
		
		File f = new File(datasetName);
		int fileSize = (int) f.length();
		
		RandomAccessFile aFile = null;
		FileChannel inChannel = null;
		MappedByteBuffer buffer = null;
		
		int[] fileType = new int[1];
		int[] encoding = new int[1];
		int[] segmentNo = new int[1];
		int[] datasize = new int[1];
		short[] decimals = new short[1];
		
		int read = -1;
		int i = -1;
		try {	
			CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			aFile = new RandomAccessFile(datasetName, "r");
			inChannel = aFile.getChannel();
			//buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			// set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
			
            //System.out.println("Checking entire range of characters in multi thread mode");
			// offset to current position
			
			int count = (fileSize - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
			for (int j = 0; j < noDistributions + 1; j++)
				b[j] = new BitSet(count);
			
			if (datasize[0] == 1) { // single byte
				System.out.println("Single Byte read");
				System.out.println("Number of records : " + count);
				System.out.println("Number of distributions : " + noDistributions);
				for (i = 0; i < count; i++) {
					//if (i % 100000 == 0)
					//	System.out.println(i);
					
					// read each character byte
					read = buffer.get();
					
					//if (i % 100000000 == 0)
					//	System.out.println(datasetName + ": Read, " + read + " at position : " + i);
					
					if (read >= 0 && read < noDistributions + 1) {
						b[read].set(i);
						//if (i % 100000000 == 0)
						//	System.out.println(datasetName + ": Read, " + read + " at position : " + i);
						
					}
				}
			}
			else if (datasize[0] == 2) { // double byte
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getShort();
					
					if (read >= 0 && read < noDistributions)
						b[read].set(i);
				}
			}
			else if (datasize[0] == 4) { // four bytes
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getInt();
	    		
					if (read >= 0 && read < noDistributions)
						b[read].set(i);
				}
			}
		}
		catch (Exception e) {
			System.out.println("Exception at position = " + i);
			e.printStackTrace();
		}
		finally {
			try {
				if (buffer != null)
					buffer = null;
			
				if (inChannel != null) {
					inChannel.close();
					inChannel = null;
				}
			
				if (aFile != null) {
					aFile.close();
					aFile = null;
				}
			}
			catch (Exception e) {
				; // ignore
			}
		}
		
    	return b;
    	
	}
	
	
	/**
	 * Method to get BOSS result sets as RoaringBitmaps for the each of the unique value, value represented by the location of the
	 * BitSet in the array. When the number of distribution > 2, then use Roaring.
	 * 
	 * @param dbName Database Name
	 * @param datasetName Dataset Name
	 * @param noDistributions No of unique values associated with the Dataset Name
	 * @return An array of RoaringBitmaps
	 */
	public static RoaringBitmap[] getDataDistributionsRoaring(String dbName, String datasetName, int noDistributions) {
		RoaringBitmap[] b = new RoaringBitmap[noDistributions + 1];
		
		File f = new File(datasetName);
		int fileSize = (int) f.length();
		
		RandomAccessFile aFile = null;
		FileChannel inChannel = null;
		MappedByteBuffer buffer = null;
		
		int[] fileType = new int[1];
		int[] encoding = new int[1];
		int[] segmentNo = new int[1];
		int[] datasize = new int[1];
		short[] decimals = new short[1];
		
		int read = -1;
		int i = -1;
		try {	
			CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			aFile = new RandomAccessFile(datasetName, "r");
			inChannel = aFile.getChannel();
			//buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			// set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
			
            //System.out.println("Checking entire range of characters in multi thread mode");
			// offset to current position
			
			int count = (fileSize - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
			for (int j = 0; j < noDistributions + 1; j++)
				b[j] = new RoaringBitmap();
			
			if (datasize[0] == 1) { // single byte
				System.out.println("Single Byte read");
				for (i = 0; i < count; i++) {
					//if (i % 100000 == 0)
					//	System.out.println(i);
					
					// read each character byte
					read = buffer.get();
					
					if (read >= 0 && read < noDistributions + 1)
						b[read].add(i);
				}
			}
			else if (datasize[0] == 2) { // double byte
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getShort();
					
					if (read >= 0 && read < noDistributions)
						b[read].add(i);
				}
			}
			else if (datasize[0] == 4) { // four bytes
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getInt();
	    		
					if (read >= 0 && read < noDistributions)
						b[read].add(i);
				}
			}
		}
		catch (Exception e) {
			System.out.println("Exception at position = " + i);
			e.printStackTrace();
		}
		finally {
			try {
				if (buffer != null)
					buffer = null;
			
				if (inChannel != null) {
					inChannel.close();
					inChannel = null;
				}
			
				if (aFile != null) {
					aFile.close();
					aFile = null;
				}
			}
			catch (Exception e) {
				; // ignore
			}
		}
		
    	return b;
    	
	}
	
	public static Hashtable<Integer, ImmutableRoaringBitmap> getDataDistributionsMutable(String dbName, String datasetName) {
		
		long beginTime = System.nanoTime();
		
		Hashtable<Integer, MutableRoaringBitmap> table = new Hashtable<Integer, MutableRoaringBitmap>();
		
		File f = new File(datasetName);
		int fileSize = (int) f.length();
		
		RandomAccessFile aFile = null;
		FileChannel inChannel = null;
		MappedByteBuffer buffer = null;
		
		int[] fileType = new int[1];
		int[] encoding = new int[1];
		int[] segmentNo = new int[1];
		int[] datasize = new int[1];
		short[] decimals = new short[1];
		
		int read = -1;
		int i = -1;
		try {	
			CheckSum.validateFile(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			aFile = new RandomAccessFile(datasetName, "r");
			inChannel = aFile.getChannel();
			//buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, CheckSum.FILE_CHECKSUM_LENGTH, (fileSize - CheckSum.FILE_CHECKSUM_LENGTH));
			// set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
			
            //System.out.println("Checking entire range of characters in multi thread mode");
			// offset to current position
			
			int count = (fileSize - CheckSum.FILE_CHECKSUM_LENGTH)/datasize[0];
			MutableRoaringBitmap b = null;
			
			if (datasize[0] == 1) { // single byte
				System.out.println("Single Byte read");
				
				for (i = 0; i < count; i++) {
					//if (i % 100000 == 0)
					//	System.out.println(i);
					
					// read each character byte
					read = buffer.get();
					
					b = table.get(read);
					if (b == null) {
						b = new MutableRoaringBitmap();
						table.put(read, b);
					}
					
					b.add(i);
				}
			}
			else if (datasize[0] == 2) { // double byte
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getShort();
					
					b = table.get(read);
					if (b == null) {
						b = new MutableRoaringBitmap();
						table.put(read, b);
					}
					
					b.add(i);
				}
			}
			else if (datasize[0] == 4) { // four bytes
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getInt();
	    		
					b = table.get(read);
					if (b == null) {
						b = new MutableRoaringBitmap();
						table.put(read, b);
					}
					
					b.add(i);
				}
			}
		}
		catch (Exception e) {
			System.out.println("Exception at position = " + i);
			e.printStackTrace();
		}
		finally {
			try {
				if (buffer != null)
					buffer = null;
			
				if (inChannel != null) {
					inChannel.close();
					inChannel = null;
				}
			
				if (aFile != null) {
					aFile.close();
					aFile = null;
				}
			}
			catch (Exception e) {
				; // ignore
			}
		}
		
		long endTime = System.nanoTime();
		long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
		System.out.println("Roaring Bitmap extraction time : " + diff);
		beginTime = System.nanoTime();
		
		Hashtable<Integer, ImmutableRoaringBitmap> retTable = new Hashtable<Integer, ImmutableRoaringBitmap>();
		Enumeration<Integer> e = table.keys();
		int key = 0;
		MutableRoaringBitmap rb = null;
		while (e.hasMoreElements()) {
			key = e.nextElement();
			rb = table.remove(key);
			retTable.put(key, (ImmutableRoaringBitmap)rb);
		}
		
		//endTime = System.nanoTime();
		//diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
		//System.out.println("Mutable Roaring to Immutable Roaring conversion  : " + diff);
		
    	return retTable;
    	
	}
	
	/**
	 * Method to get data for the input filter in a non threaded mode
	 * @param filter An array of characters
	 * @return
	 */
	public BitSet getData(int[] filter) {
		try {
			this.setFilter(filter);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	public BitSet getData() throws Exception {
		try {
			if (this._singleIn || this._multiIn || this._between || this._gt || this._gtEq ||
					this._lt || this._ltEq || this._between)
				this.readData();
			else
				throw new Exception("No filters set!");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	
	public void run() {
		
		System.out.println("Thread started : " + this._classDescription);
		
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
		
		

		
		// TEST FOR THE NOT FILTER
		/*
		int lowRange = 1;
		int highRange = 500000000;
		
		int[] c = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
		int[] c1 = {1,2,3,4,5,6,7};
		int[] c2 = {1,2,3,4,5};
		int[] c3 = {2};
		int[] c4 = {1,2};
		int[] c5 = {2,3};
		int[] c6 = {1,2,3,4,5,6,12,13,14,15,16,17,18,24,25,26,27,28,29,30,36,37,38,39,40,41,42}; // state 12
		int[] c7 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100}; // County No 100
		int[] c8 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,150,175,200,225,245}; // zip no 245
		
		try {
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
			DimDataReader hr = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			hr.setNotFilter(c5);
			hr.readData();
			System.out.println("Count : " + hr.getResultBitSet().cardinality());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		*/
	}


}
