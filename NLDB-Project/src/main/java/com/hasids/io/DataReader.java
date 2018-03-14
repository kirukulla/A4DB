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

package com.hasids.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.CheckSum;
import com.hasids.datastructures.DataGroupingObject;


public class DataReader extends Observable implements Runnable {

	//public static final int TYPE_SET = 1;
	//public static final int TYPE_MAP = 2;
	
	private String _datasetName;
	private String _dbName;
	
	private int _fileType;
	private int _encoding;
	private int _segmentNo;
	private int _dataLength;
	private int _segmentCount;
	
	private BitSet _computedBitSet = null;
	private long _elapsedTimeInMillis = 0L; 
	private int _filteredCount = 0;
	private int _filterLowRange = 1; // for beginning of file, it must be set to 1
	private int _filterHighRange = 0; // high range - exclusive
	
	private Object _filter;
	private Object _gtFilter;
	private Object _ltFilter;
	private Object _between1Filter;
	private Object _between2Filter;
	
	
	private boolean _multiIn = false;
	private boolean _singleIn = false;
	private boolean _not = false;
	private boolean _gtEq = false;
	private boolean _ltEq = false;
	private boolean _gt = false;
	private boolean _lt = false;
	private boolean _between = false;
	private boolean _multithread = false;
	
	//aggregate variables
	private long _count = 0;
	private double _sum = 0.0;
	private double _avg = 0.0;
	private double _max = Double.MIN_VALUE;
	private double _min = Double.MAX_VALUE;
	private double _mean = 0.0;
	private double _median = 0.0;
	private double _mode = 0.0;
	
	private String _classDescription;
	
	/**
	 * Default no arguments Constructor 
	 * 
	 * 
	 */
	public DataReader() {
		super();
	}
	
	/**
	 * Constructor 
	 * 
	 * @param datasetName  
	 * @throws Exception
	 */
	public DataReader(String dbName, String datasetName) throws Exception {
		this(dbName, datasetName, 1);
	}
	
	/**
	 * Constructor
	 * 
	 * @param datasetName
	 * @param lowRange
	 * @throws Exception
	 */
	public DataReader(String dbName, String datasetName, int lowRange) throws Exception {
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
	public DataReader(String dbName, String datasetName, int lowRange, int highRange ) throws Exception {
		
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
		
		if (fileType[0] == CheckSum.FILE_TYPE_DIM && (encoding[0] < CheckSum.DIM_ENCODE_TYPE1 || encoding[0] > CheckSum.DIM_ENCODE_TYPE3))
			throw new Exception ("Invalid encoding type in header, Dimension datasets encoding must be >= " + 
					CheckSum.DIM_ENCODE_TYPE1 + " and <= " + CheckSum.DIM_ENCODE_TYPE3);
		
		if (fileType[0] == CheckSum.FILE_TYPE_DIM && ((encoding[0] == CheckSum.DIM_ENCODE_TYPE1 && datasize[0] != 1) ||
				(encoding[0] == CheckSum.DIM_ENCODE_TYPE2 && datasize[0] != 2) ||
				(encoding[0] == CheckSum.DIM_ENCODE_TYPE3 && datasize[0] != 4)))
			throw new Exception ("Check Sum error, encoding type and and data size do not match!");

		if (fileType[0] == CheckSum.FILE_TYPE_FACT && (encoding[0] < CheckSum.FACT_ENCODE_TYPE_BYTE || encoding[0] > CheckSum.FACT_ENCODE_TYPE_ALPHAN))
			throw new Exception ("Invalid encoding type in header, Fact datasets encoding must be >= " + 
					CheckSum.FACT_ENCODE_TYPE_BYTE + " and <= " + CheckSum.FACT_ENCODE_TYPE_ALPHAN);
		
		if (fileType[0] == CheckSum.FILE_TYPE_FACT && ((encoding[0] == CheckSum.FACT_ENCODE_TYPE_BYTE && datasize[0] != CheckSum.BYTE_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_SHORT && datasize[0] != CheckSum.SHORT_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_INT && datasize[0] != CheckSum.INT_FLOAT_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_FLOAT && datasize[0] != CheckSum.INT_FLOAT_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_LONG && datasize[0] != CheckSum.LONG_DOUBLE_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_DOUBLE && datasize[0] != CheckSum.LONG_DOUBLE_LEN) ||
				(encoding[0] == CheckSum.FACT_ENCODE_TYPE_ALPHAN && datasize[0] > CheckSum.FACT_MAX_ALPHAN_LENGTH)
				))
			throw new Exception ("Check Sum error, encoding type and and data size do not match!");

		
		this._segmentCount = (int)(fileLength - CheckSum.FILE_CHECKSUM_LENGTH)/(datasize[0] + decimals[0]);
		if (highRange > _segmentCount)
			this._filterHighRange = _segmentCount;
		else
			this._filterHighRange = highRange;
		
		//System.out.println("High range of " + this._datasetName + " = " + this._filterHighRange);
		this._fileType = fileType[0];
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
	public DataReader(String dbName, String datasetName, int lowRange, int highRange,int fileType, int encoding, int segmentNo,int dataLength, short decimals ) throws Exception {
		
		this._dbName = dbName;
		this._datasetName = datasetName;
		this._fileType = fileType;
		this._filterLowRange = lowRange;
		this._filterHighRange = highRange;
		this._dataLength = dataLength;
		this._encoding = encoding;
		this._segmentNo = segmentNo;
		
		this._classDescription = this.getClass().getName() + "//Database Name: " + this._dbName +
				", Dataset Name : " + this._datasetName + ", Low range : " + this._filterLowRange + 
				", High Range : " + this._filterHighRange;
	}

	
	protected byte[] getFilterByte() {
		//return this._filterByte;
		return (byte[]) this._filter;
	}
	
	protected short[] getFilterShort() {
		//return this._filterShort;
		return (short[]) this._filter;
	}
	
	protected int[] getFilterInt() {
		//return this._filterInt;
		return (int[]) this._filter;
	}
	
	protected float[] getFilterFloat() {
		//return this._filterFloat;
		return (float[]) this._filter;
	}
	
	protected long[] getFilterLong() {
		//return this._filterLong;
		return (long[]) this._filter;
	}
	
	protected double[] getFilterDouble() {
		//return this._filterDouble;
		return (double[]) this._filter;
	}
	
	protected String[] getFilterString() {
		//return this._filterString;
		return (String[]) this._filter;
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
	 * Returns the file type associated with the dataset. T
	 * 
	 * @return filter Encoding
	 */
	public int getFiletype () {
		return this._fileType;
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
	
	/**
	 * Set an array of characters as filters for data read operations.
	 * 
	 * @param c Array of characters
	 */
	protected void setFilter(int[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		
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
		this._not = not;
	}
	
	protected void setFilter(byte[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		this._not = not;
	}
	
	protected void setFilter(short[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		this._not = not;
	}
	
	protected void setFilter(long[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		this._not = not;
	}
	
	protected void setFilter(float[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		this._not = not;
	}
	
	protected void setFilter(double[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		this._not = not;
	}
	
	protected void setFilter(String[] c, boolean not) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");

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
		
		
		for(int i = 0; i < c.length; i++)
			if (c[i].getBytes().length > this._dataLength)
				throw new Exception("Data length of filter exceeds field data length!");
		
		this._filter = c;
		this._not = not;
	}
	
	/**
	 * Set the character to get all record ids greater than the set character
	 * 
	 * @param c Greater Than character filter 
	 */
	protected void setGTFilter(int c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(byte c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(short c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(long c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(float c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(double c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTFilter(String c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gt = true;
	}
	
	protected void setGTEQFilter(int c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(byte c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(short c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(long c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(float c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(double c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	protected void setGTEQFilter(String c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._gtFilter = c;
		this._gtEq = true;
	}
	
	/**
	 * Set the character to get all record ids lesser than the set character
	 * 
	 * @param c Lesser Than character filter 
	 */
	protected void setLTEQFilter(int c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTFilter(byte c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(short c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(long c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(float c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(double c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(String c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTFilter(int c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._lt = true;
	}
	
	protected void setLTEQFilter(byte c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTEQFilter(short c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTEQFilter(long c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTEQFilter(float c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTEQFilter(double c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");
		this._ltFilter = c;
		this._ltEq = true;
	}
	
	protected void setLTEQFilter(String c) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");
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
	protected void setBETWEENFilter(int c1, int c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_INT && this._encoding != CheckSum.DIM_ENCODE_TYPE3)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(byte c1, byte c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_BYTE && this._encoding != CheckSum.DIM_ENCODE_TYPE1)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(short c1, short c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_SHORT && this._encoding != CheckSum.DIM_ENCODE_TYPE2)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(long c1, long c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_LONG)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(float c1, float c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_FLOAT)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(double c1, double c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			throw new Exception ("Mismatch between filter type and encoding type");
		
		this._between1Filter = c1;
		this._between2Filter = c2;
		
		this._between = true;
	}
	
	protected void setBETWEENFilter(String c1, String c2) throws Exception {
		if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			throw new Exception ("Mismatch between filter type and encoding type");
		
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
	protected Object getDataValues(int[] positions) throws Exception {
		int low = HASIDSConstants.DIM_MAX_RECORDS + 1;
		int high = -1;
		
		if (positions == null || positions.length <= 0)
			throw new Exception ("Input positions cannot be null or empty");
		
		for (int i = 0; i < positions.length; i++) {
			if (positions[i] < 1)
				throw new Exception("Positions for getting values cannot be < 1");
			
			if (positions[i] < low)
				low = positions[i];
			
			if (positions[i] > high)
				high = positions[i];
		}
		
		if ((this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE1) ||
				(this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE))
			
			return this.readValuesByte(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE2) ||
				(this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT))
			
			return this.readValuesShort(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE3) ||
				(this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_INT))
			
			return this.readValuesInt(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT))
			
			return this.readValuesFloat(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG))
			
			return this.readValuesLong(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE))
			
			return this.readValuesDouble(positions, low, high);
		
		else if ((this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN))
			
			return this.readValuesString(positions, low, high);
		
		throw new Exception("Invalid file format to read values!");
			
	}
	
	private void computeRunningStats(double value) {
		// count
		++this._count;
		
		// sum
    	this._sum = this._sum + value;
    	
    	// max
    	if (value > this._max)
    		this._max = value;
    	
    	// min
    	if (value < this._min)
    		this._min = value;
	}
	
	private void computeFinalStats() {
		this._avg = this._sum / this._count;
	}
	
	/**
	 * Method that returns the values associated with the input positions
	 * 
	 * @param positions An array of positions whose values must be returned
	 * @return An array of values matching the input positions
	 * 
	 * @throws Exception
	 */
	private byte[] readValuesByte (int[] positions, int low, int high) throws Exception {
		System.out.println("Read Values Byte");
		byte[] values = new byte[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.get(positions[i] - 1);// zero offset adjustment
         
            	// compute stats
            	this.computeRunningStats(values[i]);
            	
            	
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}
	
	private short[] readValuesShort (int[] positions, int low, int high) throws Exception {
		
		short[] values = new short[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.getShort((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}

	private int[] readValuesInt (int[] positions, int low, int high) throws Exception {
		
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
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.getInt((positions[i] - 1) * this._dataLength);
            }
            
            
            // clear the buffer
            buffer = null;
            // close the channel
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
        } catch (IOException ioe) {
            throw new IOException(ioe);
        } finally {
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}	
	
	
	private float[] readValuesFloat (int[] positions, int low, int high) throws Exception {
		
		float[] values = new float[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.getFloat((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}	

	protected int[] readFloatValuesAsInt (int[] positions, int low, int high) throws Exception {
		
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
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = (int)buffer.getFloat((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}
	
	
	protected Hashtable<Integer, ImmutableRoaringBitmap> readFloatValuesAsGroupedInt (int noParallelReadThreads) throws Exception {
		
		Hashtable<Integer, ImmutableRoaringBitmap> h = new Hashtable<Integer, ImmutableRoaringBitmap>();
		
		/*********************************************************************************
		// WHEN USING INTEGER ARRAYS TO ACCOUNT FOR STREAM BASED PROCESSING OF AGGREGATION
		// TOTAL TIME FOR OPERATIONS : FILE READ INTO INTEGER ARRAY : 5 SECS TO 29 SECS
        // TOTAL TIME FOR GROUPING DATA : 300 SECS/5 MINUTES to 1040 SECS/17 MINUTES 
        **********************************************************************************/
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		if (noParallelReadThreads > HASIDSConstants.MAX_PARALLELFILEREAD_THREADS)
			throw new Exception ("Number of maximum parallel read thread distribution per file cannot exceed " + HASIDSConstants.MAX_PARALLELFILEREAD_THREADS);
	
		
		// break the file into multiple chunks and then read
		int recordSize = this._filterHighRange - this._filterLowRange + 1;
		int rangeSize = recordSize/noParallelReadThreads;
		
		// 
		/*MutableRoaringBitmap values[] = new MutableRoaringBitmap[noGroupingValues];
		for (int i = 0; i < values.length; i++)
			values[i] = new MutableRoaringBitmap();
		
		for(int i = 0; i < values.length; i++) {
        	System.out.println("Key = " + i);
        	MutableRoaringBitmap mrb = values[i];
        	if (mrb == null)
        		System.out.println("Null value for key = " + i);
        	else
        		System.out.println("Number of ids for key : " + i + " = " + mrb.getCardinality());
        }*/
		
		int ranges[][] = new int[noParallelReadThreads][2];
		
		for (int i = 0; i < ranges.length; i++) {
			if (i == 0) {
				ranges[i][0] = this._filterLowRange;
				ranges[i][1] = ranges[i][0] - 1 + rangeSize;
			}
			else {
				if (i == (ranges.length - 1)) {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = this._filterHighRange;
				}
				else {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = ranges[i][0] - 1 + rangeSize;
				}
			}
			
		}
		
		System.out.println("Number of ranges established : " + ranges.length);
        
		try {
			ArrayList<DataReaderCastGroupingThread> al = new ArrayList<DataReaderCastGroupingThread>();
			// each range will be read in parallel
			ThreadGroup tg = new ThreadGroup("TG Cast Float to Int : " + startTime);
			
			
			for (int i = 0; i < ranges.length; i++) {
				DataReaderCastGroupingThread drt = new DataReaderCastGroupingThread (
						this._dbName, this._datasetName, ranges[i][0], ranges[i][1], this._encoding, 
						this._dataLength,  HASIDSConstants.CAST_TYPE_FLOAT_TO_INT);
				
				Thread t = new Thread(tg, drt);
				al.add(drt);
				t.setPriority(Thread.MIN_PRIORITY);
				t.start();
				
			}
			
			while(tg.activeCount() > 0)
				Thread.sleep(100);
			
			System.out.println("No of threads started : " + al.size());
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total File read time " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            startTime = System.nanoTime();
            
            // combine all the results from the various datagroupingobjects
            DataGroupingObject[] dgoArray = new DataGroupingObject[al.size()];
            for (int i = 0; i < dgoArray.length; i++)
            	dgoArray[i] = al.get(i).getGrouping();
            
            h = DataGroupingObject.getCumulativeGroupings(dgoArray);
            
            endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total cumulation of groupings " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            
            System.out.println("Number of entries in cumulative hashtable : " + h.size());
            
            /*
            Enumeration<Integer> e = h.keys();
            while(e.hasMoreElements()) {
            	int key = e.nextElement();
            	ImmutableRoaringBitmap irb = h.get(key);
            	System.out.println("Number of ids for : " + key + ", " + irb.getCardinality() + ", Min : " + irb.first() + ", Max : " + irb.last());
            }*/
            
            /*for(int i = 0; i < values.length; i++) {
            	System.out.println("Key = " + i);
            	MutableRoaringBitmap mrb = values[i];
            	if (mrb == null)
            		System.out.println("Null value for key = " + i);
            	else
            		System.out.println("Number of ids for key : " + i + " = " + mrb.getCardinality());
            }*/
            
            /*startTime = System.nanoTime();
            List<Integer> valuesList = Arrays.asList(values);
            // grouping of the data
            Map<Integer, List<Integer>> byGender = valuesList.stream().collect(Collectors.groupingBy(Integer::intValue));
    		
            endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total Grouping time using stream grouping " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            
            System.out.println("Number of keys : " + byGender.size());
            
            Set<Integer> s = byGender.keySet();
            Iterator<Integer> i = s.iterator();
            while(i.hasNext()) {
            	Integer key = i.next();
            	System.out.println("Key : " + key + ", No of entries = " + byGender.get(key).size());
            }
            
            for(Integer keys : byGender.keySet()) {
    			System.out.println(byGender.get(keys).toString());
    		}
    		*/
    		
    		
            
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return h;
	}
	
	/*
	protected void readFloatValuesAsGroupedInt (int noParallelReadThreads) throws Exception {
		
		//
		// TOTAL TIME FOR OPERATIONS : FILE READ INTO INTEGER ARRAY : 5 SECS TO 29 SECS
        // TOTAL TIME FOR GROUPING DATA : 300 SECS to 1040 SECS 
        //
		
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		if (noParallelReadThreads > HASIDSConstants.MAX_PARALLELFILEREAD_THREADS)
			throw new Exception ("Number of maximum parallel read thread distribution per file cannot exceed " + HASIDSConstants.MAX_PARALLELFILEREAD_THREADS);
	
		
		// break the file into multiple chunks and then read
		int recordSize = this._filterHighRange - this._filterLowRange + 1;
		int rangeSize = recordSize/noParallelReadThreads;
		Integer values[] = new Integer[recordSize];
		
		int ranges[][] = new int[noParallelReadThreads][2];
		
		for (int i = 0; i < ranges.length; i++) {
			if (i == 0) {
				ranges[i][0] = this._filterLowRange;
				ranges[i][1] = ranges[i][0] - 1 + rangeSize;
			}
			else {
				if (i == (ranges.length - 1)) {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = this._filterHighRange;
				}
				else {
					ranges[i][0] = ranges[i-1][1] + 1;
					ranges[i][1] = ranges[i][0] - 1 + rangeSize;
				}
			}
			
		}
        
		try {
			ArrayList<DataReaderCastGroupingThread> al = new ArrayList<DataReaderCastGroupingThread>();
			// each range will be read in parallel
			ThreadGroup tg = new ThreadGroup("TG Cast Float to Int : " + startTime);
			
			
			for (int i = 0; i < ranges.length; i++) {
				DataReaderCastGroupingThread drt = new DataReaderCastGroupingThread (
						this._dbName, this._datasetName, ranges[i][0], ranges[i][1], this._encoding, 
						this._dataLength,  HASIDSConstants.CAST_TYPE_FLOAT_TO_INT, values);
				
				Thread t = new Thread(tg, drt);
				al.add(drt);
				t.setPriority(Thread.NORM_PRIORITY);
				t.start();
				
			}
			
			while(tg.activeCount() > 0)
				Thread.sleep(100);
			
			System.out.println("No of threads started : " + al.size());
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total File read time " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            
            startTime = System.nanoTime();
            List<Integer> valuesList = Arrays.asList(values);
            // grouping of the data
            Map<Integer, List<Integer>> byGender = valuesList.stream().collect(Collectors.groupingBy(Integer::intValue));
    		
            endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("Total Grouping time using stream grouping " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            
            System.out.println("Number of keys : " + byGender.size());
            
            Set<Integer> s = byGender.keySet();
            Iterator<Integer> i = s.iterator();
            while(i.hasNext()) {
            	Integer key = i.next();
            	System.out.println("Key : " + key + ", No of entries = " + byGender.get(key).size());
            }
            
            //for(Integer keys : byGender.keySet()) {
    		//	System.out.println(byGender.get(keys).toString());
    		//}
    		
    		
            
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	*/
	private long[] readValuesLong (int[] positions, int low, int high) throws Exception {
		
		long[] values = new long[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.getLong((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}
	
	private double[] readValuesDouble (int[] positions, int low, int high) throws Exception {
		
		double[] values = new double[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = buffer.getDouble((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}	
	
	protected long[] readDoubleValuesAsLong (int[] positions, int low, int high) throws Exception {
		
		long[] values = new long[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = (long)buffer.getDouble((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}

	protected int[] readDoubleValuesAsInt (int[] positions, int low, int high) throws Exception {
		
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
            int mapSize = (high - low + 1) * this._dataLength;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	values[i] = (int)buffer.getDouble((positions[i] - 1) * this._dataLength);
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
	}

	private String[] readValuesString (int[] positions, int low, int high) throws Exception {
		
		String[] values = new String[positions.length];
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	this._filteredCount = 0;
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (high - low + 1) * this._dataLength;
            int dataLen = 0;
            byte[] b = null;
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (low - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // read the file with the input positions
            for (int i = 0; i < positions.length; i++) {
            	// set the buffer position
            	buffer.position((positions[i] - 1) * this._dataLength);
            	// get the actual data length of each string value; the first byte in the data length 
            	// is a byte containing the length of the actual data
                dataLen = buffer.get();
                if (dataLen < 0)
                	dataLen = dataLen + 256;
                
                // create a new byte buffer equal to the data length
                b = new byte[dataLen];
                
            	buffer.get(b);
            	values[i] = new String(b, "UTF-8");
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            this._elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + this._elapsedTimeInMillis);
        }
        
        return values;
        
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
		
		if (this._computedBitSet != null)
			retVal = this._computedBitSet.stream().toArray();
		
		long endTime = System.nanoTime();
		long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to convert from BitSet to Array " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
		return retVal;
	}
	
	public RoaringBitmap getResultSetBitmap() throws Exception {
		long beginTime = System.nanoTime();
		RoaringBitmap retVal = null;
		if (this._multithread)
			throw new Exception("This method must be called only when running in a single thread mode, use the calling wrapper for the Array of matching record ids!");
		
		if (this._computedBitSet != null) {
			retVal = new RoaringBitmap();
			retVal.limit(this._computedBitSet.cardinality());
			
			int values[] = this._computedBitSet.stream().toArray();
			int len = values.length;
			for(int i = 0; i < len; i++)
				retVal.add(values[i]);
		
		}
		
		long endTime = System.nanoTime();
		long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
        System.out.println("Time to convert from BitSet to RoaringBitmap " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
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
	 * Method to get a list of all record ids that have a non null value
	 * 
	 * @param buffer MappedByteBuffer from which data will be read
	 * @param multi Flag indicating whether this is a single or multi-threaded read
	 */
	private void readDataNotNullValues(MappedByteBuffer buffer, boolean multi) {
		System.out.println("ALL NOT NULL FILTER");
		// offset to current position
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		int read;
		float readF;
		double readD;
		long readL;
		int count = buffer.limit()/this._dataLength;
		
		if (this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE1) {
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE2) {
			for (int i = 0; i < count; i++) {
		
				// read each character byte
				read = buffer.getShort();
		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_DIM && this._encoding == CheckSum.DIM_ENCODE_TYPE3) {
			for (int i = 0; i < count; i++) {
		
				// read each character byte
				read = buffer.getInt();
		
				// ignore the nulls
				if (read != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				if (read != Byte.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
			for (int i = 0; i < count; i++) {
		
				// read each character byte
				read = buffer.getShort();
		
				// ignore the nulls
				if (read != Short.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
			for (int i = 0; i < count; i++) {
		
				// read each character byte
				read = buffer.getInt();
		
				// ignore the nulls
				if (read != Integer.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
			for (int i = 0; i < count; i++) {
		    		
				// read each character byte
				readF = buffer.getFloat();
    		
				// ignore the nulls
				if (readF != Float.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
			for (int i = 0; i < count; i++) {
		    		
				// read each character byte
				readD = buffer.getDouble();
    		
				// ignore the nulls
				if (readD != Double.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
			for (int i = 0; i < count; i++) {
		    		
				// read each character byte
				readL = buffer.getLong();
    		
				// ignore the nulls
				if (readL != Long.MIN_VALUE)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT && this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
			int dataLen;
			for (int i = 0; i < count; i++) {
		    	
				buffer.position(i * this._dataLength);
				dataLen = buffer.get();
				
				if (dataLen != 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
    	// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) != 0)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
		
	/**
	 * Method to get the record ids matching the filter set
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating if the read is single or multi-threaded
	 * 
	 */
	private void readDataIN(MappedByteBuffer buffer, boolean multi) {
		System.out.println("IN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte[] filter = (byte[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					//if (read == this._filter[j])
					if ((read ^ filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short[] filter = (short[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					//if (read == this._filter[j])
					if ((read ^ filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int[] filter = (int[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					//if (read == this._filter[j])
					if ((read ^ filter[j]) == 0) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float[] filter = (float[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					if (read == filter[j]) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long[] filter = (long[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					if (read == filter[j]) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double[] filter = (double[]) this._filter;
			int filterLength = filter.length;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				// ignore the nulls
				for (int j = 0; j < filterLength; j++)
					if (read == filter[j]) {
						_computedBitSet.set((int)(i + offset));
						break;
					}
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
			// convert the filter strings into byte streams
			String[] filter = (String[]) this._filter;
			int noFilters = filter.length;
			byte[][] byteFilter = new byte[noFilters][];
			int[] byteFilterLength = new int[noFilters];
			for (int i = 0; i < noFilters; i++) {
				// get the bytes; we will always do a byte comparison for alphanumeric
				byteFilter[i] = filter[i].getBytes();
				byteFilterLength[i] = byteFilter[i].length;
			}
			
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			for (int i = 0; i < count; i++) {
	    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
				
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					
					// now check each of the filters against the read value
					for (int j = 0; j < noFilters; j++) {
						if (currLength == byteFilterLength[j]) { // length of the filter matches
							if (b == null) { // if the data buffer is null, which means we have not read data from file yet
								b = new byte[currLength]; // create a new buffer
								buffer.get(b); // load the buffer
							}
							
							// now check if the first and last bytes match before we check for the rest
							if ((currLength == 1 && b[0] ==  byteFilter[j][0]) ||
									(currLength == 2 && b[0] ==  byteFilter[j][0] && b[1] == byteFilter[j][1])) { 
								_computedBitSet.set((int)(i + offset));
								break; // get out of the for loop
							}
							else if(currLength > 2 && b[0] ==  byteFilter[j][0] && b[currLength - 1] == byteFilter[j][currLength - 1]) {
								// first and last byte have matched
								if (Arrays.equals(b, byteFilter[j])) {
									_computedBitSet.set((int)(i + offset));
									break; // get out of the for loop
								}
									
							}
						}
					}
				}
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
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
    	}*/
	}
	
	/**
	 * Method to get the record ids matching the single character filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating if the read is single or multi-threaded
	 * 
	 */
	private void readDataSingleCheck(MappedByteBuffer buffer, boolean multi) {
		System.out.println("SINGLE CHECK FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte[] filter = (byte[]) this._filter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				//if (read == _filter[0])
				if ((read ^ filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short[] filter = (short[]) this._filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				//if (read == _filter[0])
				if ((read ^ filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int[] filter = (int[]) this._filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				//if (read == _filter[0])
				if ((read ^ filter[0]) == 0)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float[] filter = (float[]) this._filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				//if (read == _filter[0])
				if (read == filter[0])
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long[] filter = (long[]) this._filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				//if (read == _filter[0])
				if (read == filter[0])
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double[] filter = (double[]) this._filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				//if (read == _filter[0])
				if (read == filter[0])
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
			// convert the filter strings into byte streams
			String[] filter = (String[]) this._filter;
			int noFilters = filter.length;
			byte[][] byteFilter = new byte[noFilters][];
			for (int i = 0; i < noFilters; i++) {
				// get the bytes; we will always do a byte comparison for alphanumeric
				byteFilter[i] = filter[i].getBytes();
			}
			
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			for (int i = 0; i < count; i++) {
	    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
				
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					
					// now check each of the filters against the read value
					if (currLength == byteFilter[0].length) { // length of the filter matches
						if (b == null) { // if the data buffer is null, which means we have not read data from file yet
							b = new byte[currLength]; // create a new buffer
							buffer.get(b); // load the buffer
						}
							
						// now check if the first and last bytes match before we check for the rest
						if ((currLength == 1 && b[0] ==  byteFilter[0][0]) ||
								(currLength == 2 && b[0] ==  byteFilter[0][0] && b[1] == byteFilter[0][1])) { 
							_computedBitSet.set((int)(i + offset));
							break; // get out of the for loop
						}
						else if(currLength > 2 && b[0] ==  byteFilter[0][0] && b[currLength - 1] == byteFilter[0][currLength - 1]) {
							// first and last byte have matched
							if (Arrays.equals(b, byteFilter[0])) {
								_computedBitSet.set((int)(i + offset));
								break; // get out of the for loop
							}		
						}
					}
				}
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) == _filter[0])
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	/**
	 * Method to get record ids whose value is greater than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGT(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare = (Byte)_gtFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare = (Short)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare = (Integer)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare = (Float)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read > compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) { // four bytes
			// convert the filter strings into byte streams
			String filter = (String) this._gtFilter;
			byte[] compare = new byte[filter.length()];
			int compareLength = compare.length;
						
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			int chkLen = 0;
			boolean found = false;
			int j = 0;
			for (int i = 0; i < count; i++) {
				// reset boolean
				found = false;    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
							
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
			    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					b = new byte[currLength]; // create a new buffer
					buffer.get(b);
					chkLen = (currLength > compareLength ? compareLength : currLength);
					for (j = 0; j < chkLen; j++) {
						if (b[j] == compare[j])
							continue;
						else if (b[j] < compare[j])
							break;
						else {
							found = true;
							_computedBitSet.set((int)(i + offset));
							break;
						}
					}
					
					if (found == false && j == chkLen && currLength > compareLength)
						_computedBitSet.set((int)(i + offset));
				}	
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	private void readDataGTEQ(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare = (Byte)_gtFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare = (Short)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare = (Integer)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare = (Float)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read >= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) { // four bytes
			// convert the filter strings into byte streams
			String filter = (String) this._gtFilter;
			byte[] compare = new byte[filter.length()];
			int compareLength = compare.length;
						
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			int chkLen = 0;
			boolean found = false;
			int j = 0;
			for (int i = 0; i < count; i++) {
				// reset boolean
				found = false;    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
							
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
			    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					b = new byte[currLength]; // create a new buffer
					buffer.get(b);
					chkLen = (currLength > compareLength ? compareLength : currLength);
					for (j = 0; j < chkLen; j++) {
						if (b[j] == compare[j])
							continue;
						else if (b[j] < compare[j])
							break;
						else {
							found = true;
							_computedBitSet.set((int)(i + offset));
							break;
						}
					}
					
					if (found == false && j == chkLen && currLength >= compareLength)
						_computedBitSet.set((int)(i + offset));
				}	
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	private void readDataLT(MappedByteBuffer buffer, boolean multi) {
		System.out.println("LT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare = (Byte)_gtFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare = (Short)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare = (Integer)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare = (Float)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read < compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) { // four bytes
			// convert the filter strings into byte streams
			String filter = (String) this._gtFilter;
			byte[] compare = new byte[filter.length()];
			int compareLength = compare.length;
						
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			int chkLen = 0;
			boolean found = false;
			int j = 0;
			for (int i = 0; i < count; i++) {
				// reset boolean
				found = false;    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
							
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
			    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					b = new byte[currLength]; // create a new buffer
					buffer.get(b);
					chkLen = (currLength > compareLength ? compareLength : currLength);
					for (j = 0; j < chkLen; j++) {
						if (b[j] == compare[j])
							continue;
						else if (b[j] > compare[j])
							break;
						else {
							found = true;
							_computedBitSet.set((int)(i + offset));
							break;
						}
					}
					
					if (found == false && j == chkLen && currLength < compareLength)
						_computedBitSet.set((int)(i + offset));
				}	
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	private void readDataLTEQ(MappedByteBuffer buffer, boolean multi) {
		System.out.println("LTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare = (Byte)_gtFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare = (Short)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare = (Integer)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare = (Float)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read <= compare)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) { // four bytes
			// convert the filter strings into byte streams
			String filter = (String) this._gtFilter;
			byte[] compare = new byte[filter.length()];
			int compareLength = compare.length;
						
			// set the byte length which will be read to one byte more than the filter
			// if the last byte is not null, then the retrieved value from buffer
			// will not match any of our filters and hence we can skip further
			// checks.
			byte[] b = null;
			int currLength = 0;
			int chkLen = 0;
			boolean found = false;
			int j = 0;
			for (int i = 0; i < count; i++) {
				// reset boolean
				found = false;    		
				// set the position of the buffer
				buffer.position(i * this._dataLength);
				b = null; // reset the buffer
							
				// read bytes into the buffer
				currLength = buffer.get();
				if (currLength < 0)
					currLength = currLength + 256;
			    		
				// check if the data length exceeds the max filter length
				if (currLength > 0) { // if data in file is not null
					b = new byte[currLength]; // create a new buffer
					buffer.get(b);
					chkLen = (currLength > compareLength ? compareLength : currLength);
					for (j = 0; j < chkLen; j++) {
						if (b[j] == compare[j])
							continue;
						else if (b[j] > compare[j])
							break;
						else {
							found = true;
							_computedBitSet.set((int)(i + offset));
							break;
						}
					}
					
					if (found == false && j == chkLen && currLength <= compareLength)
						_computedBitSet.set((int)(i + offset));
				}	
			}
		}
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}	

	/**
	 * Method to get record ids whose value is between the low and high of the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataBETWEEN(MappedByteBuffer buffer, boolean multi) {
		System.out.println("BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare1 = (Byte)_between1Filter;
			byte compare2 = (Byte)_between2Filter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare1 = (Short)_between1Filter;
			short compare2 = (Short)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare1 = (Integer)_between1Filter;
			int compare2 = (Integer)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare1 = (Float)_between1Filter;
			float compare2 = (Float)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare1 = (Long)_between1Filter;
			long compare2 = (Long)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare1 = (Double)_between1Filter;
			double compare2 = (Double)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read >= compare1 && read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	/**
	 * Method to get record ids whose value is greater than the input filter and lesset than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTOrLT(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GT OR LT FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare1 = (Byte)_gtFilter;
			byte compare2 = (Byte)_ltFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare1 = (Short)_gtFilter;
			short compare2 = (Short)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare1 = (Integer)_gtFilter;
			int compare2 = (Integer)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare1 = (Float)_gtFilter;
			float compare2 = (Float)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare1 = (Long)_ltFilter;
			long compare2 = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare1 = (Double)_ltFilter;
			double compare2 = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read > compare1 || read < compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}

	/**
	 * Method to get record ids whose value is greater than the input filter and lesset than the input filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTEQOrLTEQ(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GTEQ OR LTEQ FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare1 = (Byte)_gtFilter;
			byte compare2 = (Byte)_ltFilter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare1 = (Short)_gtFilter;
			short compare2 = (Short)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare1 = (Integer)_gtFilter;
			int compare2 = (Integer)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare1 = (Float)_gtFilter;
			float compare2 = (Float)_ltFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare1 = (Long)_ltFilter;
			long compare2 = (Long)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare1 = (Double)_ltFilter;
			double compare2 = (Double)_gtFilter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read >= compare1 || read <= compare2)
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}
	
	/**
	 * Method to get record ids whose value is greater than the input GT filter, lesser than the input LT filter
	 * and in between the low and high of the input BETWEEN filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTOrLTOrBETWEEN(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GT OR LT OR BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare1 = (Byte)_gtFilter;
			byte compare2 = (Byte)_ltFilter;
			byte between1 = (Byte)_between1Filter;
			byte between2 = (Byte)_between2Filter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare1 = (Short)_gtFilter;
			short compare2 = (Short)_ltFilter;
			short between1 = (Short)_between1Filter;
			short between2 = (Short)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare1 = (Integer)_gtFilter;
			int compare2 = (Integer)_ltFilter;
			int between1 = (Integer)_between1Filter;
			int between2 = (Integer)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare1 = (Float)_gtFilter;
			float compare2 = (Float)_ltFilter;
			float between1 = (Float)_between1Filter;
			float between2 = (Float)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare1 = (Long)_ltFilter;
			long compare2 = (Long)_gtFilter;
			long between1 = (Long)_between1Filter;
			long between2 = (Long)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare1 = (Double)_ltFilter;
			double compare2 = (Double)_gtFilter;
			double between1 = (Double)_between1Filter;
			double between2 = (Double)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read > compare1 || read < compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}

	/**
	 * Method to get record ids whose value is greater than the input GT filter, lesser than the input LT filter
	 * and in between the low and high of the input BETWEEN filter
	 * 
	 * @param buffer MappedByteBuffer from where data will be read
	 * @param multi Flag indicating whether the read is single or multi-threaded
	 */
	private void readDataGTEQOrLTEQOrBETWEEN(MappedByteBuffer buffer, boolean multi) {
		System.out.println("GTEQ OR LTEQ OR BETWEEN FILTER");
		int offset = 0;
		if (multi)
			offset = this._filterLowRange -1;
		
		
		int count = buffer.limit()/this._dataLength;
		
		if (this._dataLength == 1 && (this._encoding == CheckSum.DIM_ENCODE_TYPE1 || this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE)) { // single byte
			byte read;
			byte compare1 = (Byte)_gtFilter;
			byte compare2 = (Byte)_ltFilter;
			byte between1 = (Byte)_between1Filter;
			byte between2 = (Byte)_between2Filter;
			for (int i = 0; i < count; i++) {
    		
				// read each character byte
				read = buffer.get();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 2 && (this._encoding == CheckSum.DIM_ENCODE_TYPE2 || this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT)) { // double byte
			short read;
			short compare1 = (Short)_gtFilter;
			short compare2 = (Short)_ltFilter;
			short between1 = (Short)_between1Filter;
			short between2 = (Short)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getShort();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && (this._encoding == CheckSum.DIM_ENCODE_TYPE3 || this._encoding == CheckSum.FACT_ENCODE_TYPE_INT)) { // four bytes
			int read;
			int compare1 = (Integer)_gtFilter;
			int compare2 = (Integer)_ltFilter;
			int between1 = (Integer)_between1Filter;
			int between2 = (Integer)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getInt();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 4 && this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
			float read;
			float compare1 = (Float)_gtFilter;
			float compare2 = (Float)_ltFilter;
			float between1 = (Float)_between1Filter;
			float between2 = (Float)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getFloat();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) { // four bytes
			long read;
			long compare1 = (Long)_ltFilter;
			long compare2 = (Long)_gtFilter;
			long between1 = (Long)_between1Filter;
			long between2 = (Long)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getLong();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		else if (this._dataLength == 8 && this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) { // four bytes
			double read;
			double compare1 = (Double)_ltFilter;
			double compare2 = (Double)_gtFilter;
			double between1 = (Double)_between1Filter;
			double between2 = (Double)_between2Filter;
			for (int i = 0; i < count; i++) {
	    		
				// read each character byte
				read = buffer.getDouble();
    		
				if (read >= compare1 || read <= compare2 || (read >= between1 && read <= between2))
					_computedBitSet.set((int)(i + offset));
			}
		}
		
		
		// get any locked original records
    	/*Hashtable<Integer, Integer> h = DimDataWriter.getLockedKeys(this._dbName, this._datasetName);
    	Enumeration<Integer> e = h.keys();
    	int key = -1;
    	while (e.hasMoreElements()) {
    		key = e.nextElement();
    		if (key >= this._filterLowRange && key <= this._filterHighRange && h.get(key) > _gtFilter)
    			_computedBitSet.set(key + offset);
    		else
    			_computedBitSet.set(key + offset, false);
    	}*/
	}	

	/*
	private void checkFiltersByte (boolean[] all, boolean singleCheck[], boolean[] checkRange, byte[][] rangeCheck, int[] k) throws Exception {
		
		try {
			// initialize the booleans
			k[0] = 0;
			all[0] = true;
            singleCheck[0] = false;
            checkRange[0] = true;
            rangeCheck = new byte[255][255]; 
            TreeSet<Byte> t = new TreeSet<Byte>();
            byte[] filter = (byte[])this._filter;
            
            // filter inputs division into ranges for efficiency
            byte startPoint = Byte.MIN_VALUE;
            byte endPoint = Byte.MIN_VALUE;
            
            
        	// reset counters
        	this._filteredCount = 0;
        	
        	// Read all the bytes other than null if there is no filter
            
            if (filter != null && filter.length > 1)
            	all[0] = false;
            
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (filter != null && filter.length == 1) { // only one item
            	singleCheck[0] = true;
            }
            
            
            if (filter != null && filter.length > 1) {
            	
            	rangeCheck = new byte[filter.length][filter.length];
            	
            	// re-orient the filter to be sorted
            	for (int i = 0; i < filter.length; i++) {
            		t.add(filter[i]);
            	}
            	
            	// read from the sortedset in ascending order
            	Iterator<Byte> it = t.iterator();
            	byte temp = -1, prevtemp = -1;
            	while(it.hasNext()) {
            		temp = it.next();
            		if (startPoint == Byte.MIN_VALUE) {
            			startPoint = temp;
            			rangeCheck[k[0]][0] = startPoint;
            		}
            		
            		if (endPoint == Byte.MIN_VALUE) {
            			endPoint = temp;
            			rangeCheck[k[0]][1] = endPoint;
            		}
            		else {
            			if (temp == (prevtemp + 1)) {
            				endPoint = temp;
            				rangeCheck[k[0]][1] = endPoint;
            			}
            			else {
            				++k[0];
            				startPoint = temp;
            				endPoint = temp;
            				rangeCheck[k[0]][0] = temp;
            				rangeCheck[k[0]][1] = temp;
            			}
            		}
            		prevtemp = temp;
            	}
            }
            
            // once the ranges are established, determine if they are contiguous
            // or fragmented. If they are fragmented, it is better to loop through individual
            // filter checks instead of range checks
            int countFragmented = 0;
            for (int i = 0; i <= k[0]; i++) {
            	if (rangeCheck[i][0] == rangeCheck[i][1])
            		++countFragmented;
            }
            
            
            if (countFragmented > 0)
            	checkRange[0] = false;			
            
            
        } catch (Exception ioe) {
            throw new Exception(ioe);
        }
        
	}
	
	private void checkFiltersShort (boolean[] all, boolean singleCheck[], boolean[] checkRange, short[][] rangeCheck, int[] k) throws Exception {
		
		try {
			// initialize the booleans
			k[0] = 0;
			all[0] = true;
            singleCheck[0] = false;
            checkRange[0] = true;
            rangeCheck = new short[255][255]; 
            TreeSet<Short> t = new TreeSet<Short>();
            short[] filter = (short[])this._filter;
            
            // filter inputs division into ranges for efficiency
            short startPoint = Short.MIN_VALUE;
            short endPoint = Short.MIN_VALUE;
            
            
        	// reset counters
        	this._filteredCount = 0;
        	
        	// Read all the bytes other than null if there is no filter
            
            if (filter != null && filter.length > 1)
            	all[0] = false;
            
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (filter != null && filter.length == 1) { // only one item
            	singleCheck[0] = true;
            }
            
            
            if (filter != null && filter.length > 1) {
            	
            	rangeCheck = new short[filter.length][filter.length];
            	
            	// re-orient the filter to be sorted
            	for (int i = 0; i < filter.length; i++) {
            		t.add(filter[i]);
            	}
            	
            	// read from the sortedset in ascending order
            	Iterator<Short> it = t.iterator();
            	short temp = -1, prevtemp = -1;
            	while(it.hasNext()) {
            		temp = it.next();
            		if (startPoint == Short.MIN_VALUE) {
            			startPoint = temp;
            			rangeCheck[k[0]][0] = startPoint;
            		}
            		
            		if (endPoint == Short.MIN_VALUE) {
            			endPoint = temp;
            			rangeCheck[k[0]][1] = endPoint;
            		}
            		else {
            			if (temp == (prevtemp + 1)) {
            				endPoint = temp;
            				rangeCheck[k[0]][1] = endPoint;
            			}
            			else {
            				++k[0];
            				startPoint = temp;
            				endPoint = temp;
            				rangeCheck[k[0]][0] = temp;
            				rangeCheck[k[0]][1] = temp;
            			}
            		}
            		prevtemp = temp;
            	}
            }
            
            // once the ranges are established, determine if they are contiguous
            // or fragmented. If they are fragmented, it is better to loop through individual
            // filter checks instead of range checks
            int countFragmented = 0;
            for (int i = 0; i <= k[0]; i++) {
            	if (rangeCheck[i][0] == rangeCheck[i][1])
            		++countFragmented;
            }
            
            
            if (countFragmented > 0)
            	checkRange[0] = false;			
            
            
        } catch (Exception ioe) {
            throw new Exception(ioe);
        }
        
	}
	
	private void checkFiltersInt (boolean[] all, boolean singleCheck[], boolean[] checkRange, int[][] rangeCheck, int[] k) throws Exception {
		
		try {
			// initialize the booleans
			k[0] = 0;
			all[0] = true;
            singleCheck[0] = false;
            checkRange[0] = true;
            rangeCheck = new int[255][255]; 
            TreeSet<Integer> t = new TreeSet<Integer>();
            int[] filter = (int[])this._filter;
            
            // filter inputs division into ranges for efficiency
            int startPoint = Integer.MIN_VALUE;
            int endPoint = Integer.MIN_VALUE;
            
            
        	// reset counters
        	this._filteredCount = 0;
        	
        	// Read all the bytes other than null if there is no filter
            
            
            if (filter != null && filter.length > 1)
            	all[0] = false;
            
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (filter != null && filter.length == 1) { // only one item
            	singleCheck[0] = true;
            }
            
            
            if (filter != null && filter.length > 1) {
            	
            	rangeCheck = new int[filter.length][filter.length];
            	
            	// re-orient the filter to be sorted
            	for (int i = 0; i < filter.length; i++) {
            		t.add(filter[i]);
            	}
            	
            	// read from the sortedset in ascending order
            	Iterator<Integer> it = t.iterator();
            	int temp = -1, prevtemp = -1;
            	while(it.hasNext()) {
            		temp = it.next();
            		if (startPoint == Integer.MIN_VALUE) {
            			startPoint = temp;
            			rangeCheck[k[0]][0] = startPoint;
            		}
            		
            		if (endPoint == Integer.MIN_VALUE) {
            			endPoint = temp;
            			rangeCheck[k[0]][1] = endPoint;
            		}
            		else {
            			if (temp == (prevtemp + 1)) {
            				endPoint = temp;
            				rangeCheck[k[0]][1] = endPoint;
            			}
            			else {
            				++k[0];
            				startPoint = temp;
            				endPoint = temp;
            				rangeCheck[k[0]][0] = temp;
            				rangeCheck[k[0]][1] = temp;
            			}
            		}
            		prevtemp = temp;
            	}
            }
            
            // once the ranges are established, determine if they are contiguous
            // or fragmented. If they are fragmented, it is better to loop through individual
            // filter checks instead of range checks
            int countFragmented = 0;
            for (int i = 0; i <= k[0]; i++) {
            	if (rangeCheck[i][0] == rangeCheck[i][1])
            		++countFragmented;
            }
            
            
            if (countFragmented > 0)
            	checkRange[0] = false;			
            
            
        } catch (Exception ioe) {
            throw new Exception(ioe);
        }
        
	}

	private void checkFiltersLong (boolean[] all, boolean singleCheck[], boolean[] checkRange, long[][] rangeCheck, int[] k) throws Exception {
		
		try {
			// initialize the booleans
			k[0] = 0;
			all[0] = true;
            singleCheck[0] = false;
            checkRange[0] = true;
            rangeCheck = new long[255][255]; 
            TreeSet<Long> t = new TreeSet<Long>();
            long[] filter = (long[])this._filter;
            
            // filter inputs division into ranges for efficiency
            long startPoint = Long.MIN_VALUE;
            long endPoint = Long.MIN_VALUE;
            
            
        	// reset counters
        	this._filteredCount = 0;
        	
        	// Read all the bytes other than null if there is no filter
            
            
            if (filter != null && filter.length > 1)
            	all[0] = false;
            
            // rationalize the filters into ranges from an array to do a faster range
            // based check instead of an array check
            if (filter != null && filter.length == 1) { // only one item
            	singleCheck[0] = true;
            }
            
            if (filter != null && filter.length > 1 && (this._fileType == CheckSum.FILE_TYPE_FACT && (
            		this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE || 
            		this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT ||
            		this._encoding == CheckSum.FACT_ENCODE_TYPE_INT ||
            		this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG)) ||
            		(this._fileType == CheckSum.FILE_TYPE_DIM && 
            		(this._encoding == CheckSum.DIM_ENCODE_TYPE1 ||
            		this._encoding == CheckSum.DIM_ENCODE_TYPE2 ||
            		this._encoding == CheckSum.DIM_ENCODE_TYPE3))) {
            	
            	rangeCheck = new long[filter.length][filter.length];
            	
            	// re-orient the filter to be sorted
            	for (int i = 0; i < filter.length; i++) {
            		t.add(filter[i]);
            	}
            	
            	// read from the sortedset in ascending order
            	Iterator<Long> it = t.iterator();
            	long temp = -1, prevtemp = -1;
            	while(it.hasNext()) {
            		temp = it.next();
            		if (startPoint == Long.MIN_VALUE) {
            			startPoint = temp;
            			rangeCheck[k[0]][0] = startPoint;
            		}
            		
            		if (endPoint == Long.MIN_VALUE) {
            			endPoint = temp;
            			rangeCheck[k[0]][1] = endPoint;
            		}
            		else {
            			if (temp == (prevtemp + 1)) {
            				endPoint = temp;
            				rangeCheck[k[0]][1] = endPoint;
            			}
            			else {
            				++k[0];
            				startPoint = temp;
            				endPoint = temp;
            				rangeCheck[k[0]][0] = temp;
            				rangeCheck[k[0]][1] = temp;
            			}
            		}
            		prevtemp = temp;
            	}
            	
            	// once the ranges are established, determine if they are contiguous
                // or fragmented. If they are fragmented, it is better to loop through individual
                // filter checks instead of range checks
                int countFragmented = 0;
                for (int i = 0; i <= k[0]; i++) {
                	if (rangeCheck[i][0] == rangeCheck[i][1])
                		++countFragmented;
                }
                
                
                if (countFragmented > 0)
                	checkRange[0] = false;
                
            }
            
            
        } catch (Exception ioe) {
            throw new Exception(ioe);
        }
        
	}
	*/
	
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
            
            // Temporary buffer to pull data from memory 
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning if non String
            if (this._encoding != CheckSum.FACT_ENCODE_TYPE_ALPHAN)
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
            
	        // when filter contains only one value to check, EQ clause
	        if (this._singleIn) {
	        	this.readDataSingleCheck(buffer, this._multithread);
	        }
	        
	        // check ind. filter values, multiple values, IN CLAUSE
	        else if (this._multiIn) {
	        	this.readDataIN(buffer, this._multithread);
	        }
	        
	        // gt, lt and between
	        else if (this._gt && this._lt && this._between)
            	this.readDataGTOrLTOrBETWEEN(buffer, this._multithread);	
            
	        // >=, <= and BETWEEN
	        else if (this._gtEq && this._ltEq && this._between)
            	this.readDataGTEQOrLTEQOrBETWEEN(buffer, this._multithread);
            
	        // > and <
	        else if(this._gt && this._lt)
            	this.readDataGTOrLT(buffer, this._multithread);
            
	        // >= and <=
	        else if(this._gtEq && this._ltEq)
            	this.readDataGTEQOrLTEQ(buffer, this._multithread);
            
	        // > filter
	        else if (this._gt)
            	this.readDataGT(buffer, this._multithread);
            
	        // >= filter
	        else if (this._gtEq)
            	this.readDataGTEQ(buffer, this._multithread);
            
	        // < filter
	        else if (this._lt)
            	this.readDataLT(buffer, this._multithread);
            
	        // <= filter
	        else if (this._ltEq)
            	this.readDataLTEQ(buffer, this._multithread);
            
	        // between filter
	        else if (this._between)
            	this.readDataBETWEEN(buffer, this._multithread);
	        
	        // not null positions
            else
            	this.readDataNotNullValues(buffer, this._multithread);
	        
            // close the channel
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
	
	protected BitSet getData(byte[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	protected BitSet getData(short[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	/**
	 * Method to get data for the input filter in a non threaded mode
	 * @param filter An array of characters
	 * @return
	 */
	protected BitSet getData(int[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	protected BitSet getData(long[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	protected BitSet getData(float[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	protected BitSet getData(double[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
			this.readData();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.getResultBitSet();
	}
	
	protected BitSet getData(String[] filter, boolean not) {
		try {
			this.setFilter(filter, not);
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
	
	protected static BitSet[] getDataDistributions(String dbName, String datasetName, int noDistributions) {
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
			else if (datasize[0] == 4 && encoding[0] == CheckSum.FACT_ENCODE_TYPE_INT) { // four bytes
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = buffer.getInt();
	    		
					if (read >= 0 && read < noDistributions)
						b[read].set(i);
				}
			}
			else if (datasize[0] == 4 && encoding[0] == CheckSum.FACT_ENCODE_TYPE_FLOAT) { // four bytes
				for (i = 0; i < count; i++) {
		    		
					// read each character byte
					read = (int)buffer.getFloat();
	    		
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
		
		/*
		int lowRange = 1;
		int highRange = 10;
		
		int[] c = {65};
		
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long diff = 0;
		
		try {
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\\\users\\\\dpras\\\\tempdata\\\\testCS2_1.DM";
			DimDataReader hr = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			hr.setFilter(c);
			hr.readData();
			
			datasetName = "c:\\\\users\\\\dpras\\\\tempdata\\\\testCS3_1.DM";
			DimDataReader hr1 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr1.getdatasetName());
			hr1.setFilter(c);
			hr1.readData();
			
			
			
			beginTime = System.nanoTime();
			int k = 0;
			
			System.out.println("Begin bit test");
			BitSet result = hr.getResultBitSet();
			BitSet r1 = hr1.getResultBitSet();
			
			System.out.println("Cardinality 0 : " + result.cardinality());
			System.out.println("Cardinality 1 : " + r1.cardinality());
			
			result.and(r1);
			System.out.println("Intersection 1 Cardinality : " + result.cardinality());
			
			System.out.println("End bit test");
			
			k = result.cardinality();
			System.out.println("No of records matching intersections :" + k);
			
			//long[] l = result.toLongArray();
			//System.out.println("Length of lomg Array : " + l.length);
			
				
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("File read time in millis : " + 
					(hr.getElapsedTime() + hr1.getElapsedTime()));
			//System.out.println("No of records in result BitSet : " + k);
			System.out.println("Operations time in memory in millis : " + diff);
			
		}
		catch (Exception e ) {
			e.printStackTrace();
		}
		*/
		
		
		/*int lowRange = 1;
		int highRange = 10000000;
		
		int[] c = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
		int[] c1 = {1,2,3,4,5,6,7};
		int[] c2 = {1,2,3,4,5};
		int[] c3 = {2};
		int[] c4 = {1,2};
		int[] c5 = {2,3};
		int[] c6 = {1,2,3,4,5,6,12,13,14,15,16,17,18,24,25,26,27,28,29,30,36,37,38,39,40,41,42}; // state 12
		int[] c7 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100}; // County No 100
		int[] c8 = {1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,150,175,200,225,245}; // zip no 245
		
		long beginTime = System.nanoTime();
		long endTime = System.nanoTime();
		long diff = 0;
		
		try {
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			DimDataReader hr = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			hr.setFilter(c);
			hr.readData();
			
			int[] list = hr.getResultSetArray();
			System.out.println("Cardinality of hr : " + list.length);
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\month_1.DM";
			DimDataReader hr1 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr1.getdatasetName());
			hr1.setFilter(c1);
			hr1.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\year_1.DM";
			DimDataReader hr2 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr2.getdatasetName());
			hr2.setFilter(c2);
			hr2.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
			DimDataReader hr3 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr3.getdatasetName());
			hr3.setFilter(c3);
			hr3.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\race_1.DM";
			DimDataReader hr4 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr4.getdatasetName());
			hr4.setFilter(c4);
			hr4.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\type_1.DM";
			DimDataReader hr5 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr5.getdatasetName());
			hr5.setFilter(c5);
			hr5.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\state_1.DM";
			DimDataReader hr6 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr6.getdatasetName());
			hr6.setFilter(c6);
			hr6.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\county_1.DM";
			DimDataReader hr7 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr7.getdatasetName());
			hr7.setFilter(c7);
			hr7.readData();
			
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\zip_1.DM";
			DimDataReader hr8 = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr8.getdatasetName());
			hr8.setFilter(c8);
			hr8.readData();
			
			beginTime = System.nanoTime();
			int k = 0;
			
			System.out.println("Begin bit test");
			BitSet result = hr.getResultBitSet();
			BitSet r1 = hr1.getResultBitSet();
			BitSet r2 = hr2.getResultBitSet();
			BitSet r3 = hr3.getResultBitSet();
			BitSet r4 = hr4.getResultBitSet();
			BitSet r5 = hr5.getResultBitSet();
			BitSet r6 = hr6.getResultBitSet();
			BitSet r7 = hr7.getResultBitSet();
			BitSet r8 = hr8.getResultBitSet();
			
			System.out.println("Cardinality 0 : " + result.cardinality());
			System.out.println("Cardinality 1 : " + r1.cardinality());
			System.out.println("Cardinality 2 : " + r2.cardinality());
			System.out.println("Cardinality 3 : " + r3.cardinality());
			System.out.println("Cardinality 4 : " + r4.cardinality());
			System.out.println("Cardinality 5 : " + r5.cardinality());
			System.out.println("Cardinality 6 : " + r6.cardinality());
			System.out.println("Cardinality 7 : " + r7.cardinality());
			System.out.println("Cardinality 8 : " + r8.cardinality());
			
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
			result.and(r6);
			System.out.println("Intersection 6 Cardinality : " + result.cardinality());
			result.and(r7);
			System.out.println("Intersection 7 Cardinality : " + result.cardinality());
			result.and(r8);
			System.out.println("Intersection 8 Cardinality : " + result.cardinality());
			
			System.out.println("End bit test");
			
			k = result.cardinality();
			System.out.println("No of records matching intersections :" + k);
			
			//long[] l = result.toLongArray();
			//System.out.println("Length of lomg Array : " + l.length);
			
				
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("File read time in millis : " + 
					(hr.getElapsedTime() + hr1.getElapsedTime() + hr2.getElapsedTime() + 
					hr3.getElapsedTime() + hr4.getElapsedTime() + hr5.getElapsedTime() + 
					hr6.getElapsedTime() + hr7.getElapsedTime() + hr8.getElapsedTime()));
			//System.out.println("No of records in result BitSet : " + k);
			System.out.println("Operations time in memory in millis : " + diff);
			
			int a = 128;
			byte b = (Byte) a;
			int d = b;
			System.out.println(a + " / " + b + " / " + d);
			*/
		
		
			/*int count = 31;
			
			beginTime = System.nanoTime();
			
			BitSet[] bArray = DimDataReader.getDataDistributions(dbName, datasetName, count);
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis : " + diff);
			
			for(int i = 0; i < count; i++)
				System.out.println("Cardinality for " + i + " = " + bArray[i].cardinality());
			
		}
		catch (Exception e ) {
			e.printStackTrace();
		}
		*/
		
		/*
		try {
			int lowRange = 1;
			int highRange = 10000000;
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			DimDataReader hr = new DimDataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			hr.setGTFilter(25);
			hr.setLTFilter(5);
			hr.setBETWEENFilter(15, 20);
			hr.readData();
			
			BitSet b = hr.getResultBitSet();
			System.out.println("Total count : " + b.cardinality());
						
		}
		catch (Exception e) {
			;
		}
		*/
		
		/*
		try {
			int lowRange = 1;
			int highRange = 10000000;
			int noPositions = 10000000;
			
			int positions[] = new int[noPositions];
			for (int i = 0; i < noPositions; i++)
				positions[i] = i + 1;
			
			// Get a list of male asian customers born on 5th Jul 2007
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			DataReader hr = new DataReader(dbName, datasetName, lowRange, highRange);
			System.out.println("datasetName: " + hr.getdatasetName());
			int[] values = hr.getDataValues(positions);
			
			System.out.println("Returned values : " + values.length);
			//for (int i = 0; i < noPositions; i++)
			//	System.out.println("Position (" + positions[i] + "), Value (" + i + ") : " + values[i]);
						
		}
		catch (Exception e) {
			;
		}
		*/
	}
}




class DataReaderCastGroupingThreadObj implements Runnable {
	
	String _databaseName = null;
	String _datasetName = null;
	int _filterLowRange = 0;
	int _filterHighRange = 0;
	int _dataLength = 0;
	int _type = 0;
	String _threadname = null;
	
	//Hashtable<Integer, MutableRoaringBitmap> h = new Hashtable<Integer, MutableRoaringBitmap>();
	//DataGroupingObject dgo = new DataGroupingObject(10);
	Integer[] _values = null;
	
	public DataReaderCastGroupingThreadObj(
			String databaseName, String datasetName, int low, int high, int datatype, 
			int length, int type, Integer[] values) throws Exception {
		this._filterLowRange = low;
		this._filterHighRange = high;
		this._databaseName = databaseName;
		this._datasetName = datasetName;
		
		if (datatype == CheckSum.FACT_ENCODE_TYPE_FLOAT)
				this._dataLength = 4;
		else if (datatype == CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			this._dataLength = 8;
		else if (datatype == CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			this._dataLength = length;
		else
			throw new Exception("Invalid data type received");
		
		if (type != HASIDSConstants.CAST_TYPE_DOUBLE_TO_INT && type != HASIDSConstants.CAST_TYPE_FLOAT_TO_INT )
			throw new Exception("Invalid cast type received");
		
		this._type = type;
		this._threadname = this._databaseName + "|" + this._datasetName + "(" + this._filterLowRange + ", " + this._filterHighRange + ")";
		this._values = values;
	}
	
	private void readFloatValuesAsGroupedInt () throws Exception {
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	//this.h.clear();
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            int recordCount = this._filterHighRange - this._filterLowRange + 1;
            
            //System.out.println("Start point : " + (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH);
            //System.out.println("Map size : " + mapSize);
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            //int value = 0;
            //MutableRoaringBitmap rbm = null;
            //System.out.println("Record count of grouping thread : " + recordCount + ", " + this._threadname);
            
            for (int i = 0; i < recordCount; i++) {
            	this._values[i + this._filterLowRange - 1] = (int) buffer.getFloat((i) * this._dataLength);
            	//Hashtable
            	//rbm = h.get(value);
            	//if (rbm == null) {
            	//	rbm = new MutableRoaringBitmap();
            	//	h.put(value, rbm);
            	//}
            	//rbm.add(i + this._filterLowRange -1);*/
            	
            	// Custom Array Object
            	//this.dgo.add(value, i + this._filterLowRange -1);
            	
            	// Raw Object
            	
            	
            }
            
            //System.out.println("Reading complete......");
            // clear the buffer
            buffer = null;
            // close the hannel
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
        } catch (Exception ioe) {
        	System.out.println("Exception in thread : " + this._threadname);
        	ioe.printStackTrace();
            //throw new IOException(ioe);
        } finally {
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
            System.out.println("Value at position " + this._filterLowRange + " = " + this._values[this._filterLowRange] + ", value at max length " + this._filterHighRange + " = " + this._values[this._filterHighRange]);
            
            //List<Integer> sValues = Arrays.asList(values);
            
            // get the output from grouping
            //Enumeration<Integer> e = this.h.keys();
            //int value = 0;
            //while(e.hasMoreElements()) {
            //	value = e.nextElement();
            //	System.out.println("Cardinality for " + value + " : " + h.get(value).getCardinality());
            //}
            	
            
            //System.out.println("No of entries in ArrayList : " + dgo.getEntryCount());
            //ImmutableRoaringBitmap[] groups = this.dgo.getValues();
            //for (int i = 0; i < groups.length; i++)
            //	System.out.println("Cardinality for " + i + " : " + groups[i].getCardinality());
            
            
        }
        
	}
	
	private void readDoubleValuesAsGroupedInt () throws Exception {
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	//h.clear();
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            int recordCount = this._filterHighRange - this._filterLowRange + 1;
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            int value = 0;
            MutableRoaringBitmap rbm = null;
            for (int i = 1; i <= recordCount; i++) {
            	value = (int) buffer.getDouble((i - 1) * this._dataLength);
            	//rbm = h.get(value);
            	//if (rbm == null) {
            	//	rbm = new MutableRoaringBitmap();
            	//	h.put(value, rbm);
            	//}
            	//rbm.add(i + this._filterLowRange -1);*/
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
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for " + this._datasetName + " (" + this._filterLowRange + ", " + this._filterHighRange + ") " + diff);
        }
        
	}

	//public Hashtable<Integer, MutableRoaringBitmap> getResultTable() {
	//	return this.h;
	//}
	
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Start grouping thread : " + this._threadname);
		try {
			if (this._type == HASIDSConstants.CAST_TYPE_FLOAT_TO_INT) {
				this.readFloatValuesAsGroupedInt();
			}
			else {
				this.readDoubleValuesAsGroupedInt();
			}
		}
		catch (Exception e) {
			;
		}
		System.out.println("End grouping thread : " + this._threadname);
	}
	
}


class DataReaderCastGroupingThread implements Runnable {
	
	String _databaseName = null;
	String _datasetName = null;
	int _filterLowRange = 0;
	int _filterHighRange = 0;
	int _dataLength = 0;
	int _type = 0;
	String _threadname = null;
	
	//Hashtable<Integer, MutableRoaringBitmap> h = new Hashtable<Integer, MutableRoaringBitmap>();
	DataGroupingObject _dgo = new DataGroupingObject(10);
	
	public DataReaderCastGroupingThread(
			String databaseName, String datasetName, int low, int high, int datatype, 
			int length, int type) throws Exception {
		this._filterLowRange = low;
		this._filterHighRange = high;
		this._databaseName = databaseName;
		this._datasetName = datasetName;
		
		if (datatype == CheckSum.FACT_ENCODE_TYPE_FLOAT)
				this._dataLength = 4;
		else if (datatype == CheckSum.FACT_ENCODE_TYPE_DOUBLE)
			this._dataLength = 8;
		else if (datatype == CheckSum.FACT_ENCODE_TYPE_ALPHAN)
			this._dataLength = length;
		else
			throw new Exception("Invalid data type received");
		
		if (type != HASIDSConstants.CAST_TYPE_DOUBLE_TO_INT && type != HASIDSConstants.CAST_TYPE_FLOAT_TO_INT )
			throw new Exception("Invalid cast type received");
		
		this._type = type;
		this._threadname = this._databaseName + "|" + this._datasetName + "(" + this._filterLowRange + ", " + this._filterHighRange + ")";
	}
	
	private void readFloatValuesAsGroupedInt () throws Exception {
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		int value = 0;
		int i = 0;
        try {
        	// reset counters
        	//this.h.clear();
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            int recordCount = this._filterHighRange - this._filterLowRange + 1;
            
            //System.out.println("Start point : " + (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH);
            //System.out.println("Map size : " + mapSize);
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            
            //MutableRoaringBitmap rbm = null;
            //System.out.println("Record count of grouping thread : " + recordCount + ", " + this._threadname);
            MutableRoaringBitmap rbm = null;
            for (i = 0; i < recordCount; i++) {
            	//value = (int) buffer.getFloat((i) * this._dataLength);
            	
            	// array loading
            	//this._values[i + this._filterLowRange - 1] = (int) buffer.getFloat((i) * this._dataLength);
            	
            	//Hashtable
            	//rbm = h.get(value);
            	//if (rbm == null) {
            	//	rbm = new MutableRoaringBitmap();
            	//	h.put(value, rbm);
            	//}
            	//rbm.add(i + this._filterLowRange -1);*/
            	
            	// Custom Array Object
            	this._dgo.add((int) buffer.getFloat((i) * this._dataLength), i + this._filterLowRange -1);
            	
            	// Raw Object
            	
            	
            }
            
            //System.out.println("Reading complete......");
            // clear the buffer
            buffer = null;
            // close the hannel
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
        } catch (Exception ioe) {
        	System.out.println("Exception in thread : " + this._threadname + " at position : " + i + " for value = " + value);
        	ioe.printStackTrace();
        	
            //throw new IOException(ioe);
        } finally {
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for thread " + this._threadname + " : " + diff);
            //System.out.println("Value at position " + this._filterLowRange + " = " + this._values[this._filterLowRange] + ", value at max length " + this._filterHighRange + " = " + this._values[this._filterHighRange]);
            
            //List<Integer> sValues = Arrays.asList(values);
            
            // get the output from grouping
            //Enumeration<Integer> e = this.h.keys();
            //int value = 0;
            //while(e.hasMoreElements()) {
            //	value = e.nextElement();
            //	System.out.println("Cardinality for " + value + " : " + h.get(value).getCardinality());
            //}
            	
            
            //System.out.println("No of entries in ArrayList : " + dgo.getEntryCount());
            //ImmutableRoaringBitmap[] groups = this.dgo.getValues();
            //for (int i = 0; i < groups.length; i++)
            //	System.out.println("Cardinality for " + i + " : " + groups[i].getCardinality());
            
            
        }
        
	}

	private void readDoubleValuesAsGroupedInt () throws Exception {
		
		// track the beginning time of the job
		long startTime = System.nanoTime();
		
		int value = 0;
		int i = 0;
        try {
        	// reset counters
        	//this.h.clear();
        	
        	// open file and map to memory
            RandomAccessFile aFile = new RandomAccessFile(this._datasetName, "r");
            FileChannel inChannel = aFile.getChannel();
            
            // Difference of low and high ranges with the actual data lengths
            int mapSize = (this._filterHighRange - this._filterLowRange + 1) * this._dataLength;
            int recordCount = this._filterHighRange - this._filterLowRange + 1;
            
            //System.out.println("Start point : " + (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH);
            //System.out.println("Map size : " + mapSize);
            
            // Map the file into memory
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, (this._filterLowRange - 1) * this._dataLength + CheckSum.FILE_CHECKSUM_LENGTH, mapSize);
            
            // set the byte order to LITTLE ENDIAN, the most significant bit is in the beginning
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // read the file with the input positions
            
            //MutableRoaringBitmap rbm = null;
            //System.out.println("Record count of grouping thread : " + recordCount + ", " + this._threadname);
            MutableRoaringBitmap rbm = null;
            for (i = 0; i < recordCount; i++) {
            	//value = (int) buffer.getFloat((i) * this._dataLength);
            	
            	// array loading
            	//this._values[i + this._filterLowRange - 1] = (int) buffer.getFloat((i) * this._dataLength);
            	
            	//Hashtable
            	//rbm = h.get(value);
            	//if (rbm == null) {
            	//	rbm = new MutableRoaringBitmap();
            	//	h.put(value, rbm);
            	//}
            	//rbm.add(i + this._filterLowRange -1);*/
            	
            	// Custom Array Object
            	this._dgo.add((int) buffer.getDouble((i) * this._dataLength), i + this._filterLowRange -1);
            	
            	// Raw Object
            	
            	
            }
            
            //System.out.println("Reading complete......");
            // clear the buffer
            buffer = null;
            // close the hannel
	        inChannel.close();
            // close the file
            aFile.close();
            
            buffer = null;
            
        } catch (Exception ioe) {
        	System.out.println("Exception in thread : " + this._threadname + " at position : " + i + " for value = " + value);
        	ioe.printStackTrace();
        	
            //throw new IOException(ioe);
        } finally {
        	
        	// track the ending time of the process
            long endTime = System.nanoTime();
            
            // calculate the elapsed time of the process in milli seconds
            long diff = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
            System.out.println("File based read time for thread " + this._threadname + " : " + diff);
            //System.out.println("Value at position " + this._filterLowRange + " = " + this._values[this._filterLowRange] + ", value at max length " + this._filterHighRange + " = " + this._values[this._filterHighRange]);
            
            //List<Integer> sValues = Arrays.asList(values);
            
            // get the output from grouping
            //Enumeration<Integer> e = this.h.keys();
            //int value = 0;
            //while(e.hasMoreElements()) {
            //	value = e.nextElement();
            //	System.out.println("Cardinality for " + value + " : " + h.get(value).getCardinality());
            //}
            	
            
            //System.out.println("No of entries in ArrayList : " + dgo.getEntryCount());
            //ImmutableRoaringBitmap[] groups = this.dgo.getValues();
            //for (int i = 0; i < groups.length; i++)
            //	System.out.println("Cardinality for " + i + " : " + groups[i].getCardinality());
            
            
        }
        
	}

	public DataGroupingObject getGrouping() {
		return this._dgo;
	}
	//public Hashtable<Integer, MutableRoaringBitmap> getResultTable() {
	//	return this.h;
	//}
	
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Start grouping thread : " + this._threadname);
		try {
			if (this._type == HASIDSConstants.CAST_TYPE_FLOAT_TO_INT) {
				this.readFloatValuesAsGroupedInt();
			}
			else {
				this.readDoubleValuesAsGroupedInt();
			}
		}
		catch (Exception e) {
			;
		}
		System.out.println("End grouping thread : " + this._threadname);
	}
	
}


