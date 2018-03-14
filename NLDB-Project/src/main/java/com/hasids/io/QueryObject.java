/**
 * 
 */
package com.hasids.io;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;

import com.hasids.datastructures.CheckSum;

/**
 * @author dpras
 *
 */
public class QueryObject implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7819641898054395222L;
	
	private int _fileType;
	private int _encodingType;
	
	private String _dbName;
	private String _queryName;
	private String _datasetName;
	private int _lowRange;
	private int _highRange;
	
	private QueryOperator _operator;// = new QueryOperator();

	/**
	 * 
	 */
	public QueryObject(int fileType, int encodingType, String queryName, String dbName, String datasetName, int lowRange, int highRange) throws Exception {
		// TODO Auto-generated constructor stub
		
		if (fileType != CheckSum.FILE_TYPE_DIM || fileType != CheckSum.FILE_TYPE_FACT || 
				fileType != CheckSum.FILE_TYPE_UNI)
			throw new Exception("Invalid file Type!");
		
		if (fileType == CheckSum.FILE_TYPE_DIM && (encodingType < CheckSum.DIM_ENCODE_TYPE1 ||
				encodingType > CheckSum.DIM_ENCODE_TYPE3))
			throw new Exception ("Invalid encoding type!");
		
		if (fileType == CheckSum.FILE_TYPE_FACT && (encodingType > CheckSum.FACT_ENCODE_TYPE_BYTE ||
				encodingType > CheckSum.FACT_ENCODE_TYPE_ALPHAN))
			throw new Exception ("Invalid encoding type!");
		
		if (fileType == CheckSum.FILE_TYPE_UNI && (encodingType > CheckSum.UNI_ENCODE_TYPE_OLAP ||
				encodingType > CheckSum.UNI_ENCODE_TYPE_DOC))
			throw new Exception ("Invalid encoding type!");
		
		if (queryName == null || queryName.trim().length() == 0)
			throw new Exception ("Invalid query name!");
		this._queryName = queryName;
		
		if (dbName == null || dbName.trim().length() == 0)
			throw new Exception ("Invalid database name!");
		this._dbName = dbName;
		
		if (datasetName == null || datasetName.trim().length() == 0)
			throw new Exception ("Invalid dataset name!");
		this._datasetName = datasetName;
		
		this._operator = new QueryOperator(this._fileType, this._encodingType);
	}
	
	public void setFilter(Object filter, boolean not) throws Exception{
		this._operator.setFilter(filter, not);
	}

}


class QueryOperator implements Serializable {
	
	public static String IN = "IN";
	public static String GT = "GT";
	public static String GTEQ = "GTEQ";
	public static String LT = "LT";
	public static String LTEQ = "LTEQ";
	public static String BETWEEN = "BETWEEN";
	public static String NOT = "NOT";
	
	private int _fileType;
	private int _encoding;
	private ArrayList<String> _filterList = new ArrayList<String>();
	private Object _filter = null;
	private Object _gtFilter = null;
	private Object _ltFilter = null;
	private Object _betweenFilter1 = null;
	private Object _betweenFilter2 = null;
	private boolean _notin = false;
	private boolean _notGtLt = false;
	private boolean _notBetween = false;

	public QueryOperator(int fileType, int encoding){
		this._fileType = fileType;
		this._encoding = encoding;
	}
	
	public void setFilter(Object filter, boolean not) throws Exception{
		if (this._fileType == CheckSum.FILE_TYPE_DIM) {
			if (!(filter instanceof int[]))
				throw new Exception("filter must be an array of integers");
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT) {
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				if (!(filter instanceof byte[]))
					throw new Exception("filter must be an array of bytes");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				if (!(filter instanceof short[]))
					throw new Exception("filter must be an array of shorts");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				if (!(filter instanceof int[]))
					throw new Exception("filter must be an array of integers");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				if (!(filter instanceof float[]))
					throw new Exception("filter must be an array of floats");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				if (!(filter instanceof long[]))
					throw new Exception("filter must be an array of longs");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				if (!(filter instanceof double[]))
					throw new Exception("filter must be an array of doubles");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				if (!(filter instanceof String[]))
					throw new Exception("filter must be an array of strings");
			}
		}
		
		this._filter = filter;
		this._notin = not;
		if (!this._filterList.contains(QueryOperator.IN))
			this._filterList.add(QueryOperator.IN);
		
	}
	
	public void setGTLTFilter(Object gtFilter, Object ltFilter, boolean equalFlag, boolean not) throws Exception {
		
		if (this._fileType == CheckSum.FILE_TYPE_DIM) {
			if (!((gtFilter == null || gtFilter instanceof Integer) && (ltFilter == null || ltFilter instanceof Integer)))
				throw new Exception("filters must be instances of integers");
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT) {
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				if (!((gtFilter == null || gtFilter instanceof Byte) && (ltFilter == null || ltFilter instanceof Byte)))
					throw new Exception("filters must be instances of bytes");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				if (!((gtFilter == null || gtFilter instanceof Short) && (ltFilter == null || ltFilter instanceof Short)))
					throw new Exception("filters must be instances of shorts");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				if (!((gtFilter == null || gtFilter instanceof Integer) && (ltFilter == null || ltFilter instanceof Integer)))
					throw new Exception("filters must be instances of integers");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				if (!((gtFilter == null || gtFilter instanceof Float) && (ltFilter == null || ltFilter instanceof Float)))
					throw new Exception("filters must be instances of floats");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				if (!((gtFilter == null || gtFilter instanceof Long) && (ltFilter == null || ltFilter instanceof Long)))
					throw new Exception("filters must be instances of longs");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				if (!((gtFilter == null || gtFilter instanceof Double) && (ltFilter == null || ltFilter instanceof Double)))
					throw new Exception("filters must be instances of doubles");
			}
			else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				if (!((gtFilter == null || gtFilter instanceof String) && (ltFilter == null || ltFilter instanceof String)))
					throw new Exception("filters must be instances of strings");
			}
		}
		
		
		if (gtFilter != null) {
			this._gtFilter = gtFilter;
			if (equalFlag)
				this._filterList.add(QueryOperator.GTEQ);
			else
				this._filterList.add(QueryOperator.GT);
			
		}
		
		if (ltFilter != null) {
			this._ltFilter = ltFilter;
			if (equalFlag)
				this._filterList.add(QueryOperator.LTEQ);
			else
				this._filterList.add(QueryOperator.LT);
			
		}
		
		this._notGtLt = not;
	}
	
	public void setBETWEENFilter(Object betweenFilter1, Object betweenFilter2, boolean not) throws Exception {
		
		if (betweenFilter1 == null || betweenFilter2 == null)
			throw new Exception ("BETWEEN filters cannot be null");
		
		if (this._fileType == CheckSum.FILE_TYPE_DIM) {
			if (!(betweenFilter1 instanceof Integer && betweenFilter2 instanceof Integer))
				throw new Exception("filters must be instances of integers");
		}
		else if (this._fileType == CheckSum.FILE_TYPE_FACT) {
			if (this._encoding == CheckSum.FACT_ENCODE_TYPE_BYTE) {
				if (!(betweenFilter1 instanceof Byte && betweenFilter2 instanceof Byte))
					throw new Exception("filters must be instances of bytes");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_SHORT) {
				if (!(betweenFilter1 instanceof Short && betweenFilter2 instanceof Short))
					throw new Exception("filters must be instances of shorts");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_INT) {
				if (!(betweenFilter1 instanceof Integer && betweenFilter2 instanceof Integer))
					throw new Exception("filters must be instances of integers");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_FLOAT) {
				if (!(betweenFilter1 instanceof Float && betweenFilter2 instanceof Float))
					throw new Exception("filters must be instances of floats");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_LONG) {
				if (!(betweenFilter1 instanceof Long && betweenFilter2 instanceof Long))
					throw new Exception("filters must be instances of longs");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_DOUBLE) {
				if (!(betweenFilter1 instanceof Double && betweenFilter2 instanceof Double))
					throw new Exception("filters must be instances of doubles");
			} else if (this._encoding == CheckSum.FACT_ENCODE_TYPE_ALPHAN) {
				if (!(betweenFilter1 instanceof String && betweenFilter2 instanceof String))
					throw new Exception("filters must be instances of strings");
			}
		}
		
		this._betweenFilter1 = betweenFilter1;
		this._betweenFilter2 = betweenFilter2;
		this._filterList.add(QueryOperator.BETWEEN);
		this._notBetween = not;
		
	}
	

}
