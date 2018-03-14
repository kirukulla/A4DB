/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids;

public class HASIDSConstants {

	public HASIDSConstants() {
		// TODO Auto-generated constructor stub
	}
	
	// status constants
	public static final int FAILURE = 0;
	public static final int SUCCESS = 1;

	// Operation mode constants
	public static final int OPERATION_MODE_ONLINE = 1;
	public static final int OPERATION_MODE_BATCH = 2;
	
	// Dimension constants
	public static final int DIM_MAX_RECORDS = 2000000000;

	
	public static final int DIM_ENCODE_TYPE1_MIN = Byte.MIN_VALUE + 1;
	public static final int DIM_ENCODE_TYPE1_MAX = Byte.MAX_VALUE;
	
	public static final int DIM_ENCODE_TYPE2_MIN = Short.MIN_VALUE + 1;
	public static final int DIM_ENCODE_TYPE2_MAX = Short.MAX_VALUE;
	
	public static final int DIM_ENCODE_TYPE3_MIN = Integer.MIN_VALUE + 1;
	public static final int DIM_ENCODE_TYPE3_MAX = Integer.MAX_VALUE;
	
	// fact constants
	public static final int FACT_FLOAT_MAX_RECORDS = 200000000;
	public static final int FACT_DOUBLE_MAX_RECORDS = 100000000;
	
	public static final int FACT_FLOAT_MAX_BYTES = 2000000000;
	public static final int FACT_DOUBLE_MAX_BYTES = 1900000000;
	
	public static final String FACT_FLOAT_ZERO_STRING = "0000000000";
	public static final String FACT_DOUBLE_ZERO_STRING = "0000000000000000000";
	
	public static final int FACT_MAX_ALPHAN_LENGTH = 200;
	
	// Thread status constants
	public static final short THREAD_INACTIVE = 0;
	public static final short THREAD_ACTIVE = 1;
	public static final short THREAD_COMPLETE = 2;
	public static final short THREAD_FAILED = 3;
	
	// parallelism constants
	public static final int MAX_PARALLELFILEREAD_THREADS = 2000;
	public static final int MAX_PARALLELSEGMENTREAD_THREADS = 2000;
	
	public static final short DATA_TYPE_FLOAT = 1;
	public static final short DATA_TYPE_DOUBLE = 2;
	
	// Data synchronization constants
	public static final int RETRY_LIMIT_MILLIS = 3000; // retry time out in millis
	public static final int RETRY_INCREMENT_MILLIS = 500; // retry increment time in millis
	
	// unary sets maximum BitSetSize
	public static final int UNI_MAX_BITSET_SIZE = 500000000;
	public static final int UNI_MIN_PARALLEL_RECORD_COUNT = 10000000; // 10 Million of mapping at a time.
	
	public static final int CAST_TYPE_FLOAT_TO_INT = 1;
	public static final int CAST_TYPE_DOUBLE_TO_INT = 2;
	
}
