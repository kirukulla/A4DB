/***
 * Class with methods to compute the checksum for files
 */

package com.hasids.datastructures;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.hasids.HASIDSConstants;

public class CheckSum {

	
	// position 0 = data type
	// data types in byte codes
	// DIM = byte code 1
	// FACT = byte code 2
	// UNY = byte code 3
	// LINK = byte code 4
	
	// Position 1 = encoding
	// FOR DIMENSION DATASET 
	// ASCII = byte code 1; (byte codes -128 to 127); data length = 1
	// DOUBLE BYTE = byte code 2; (byte codes -32767 to 32767); data length = 2
	// FOUR BYTE = byte code = 3; (byte codes -2.14B - 2.14B); data length = 4
	
	// FOR FACT DATASET
	// FCT BYTE  = byte code 1; Byte fact; max data length = 1; -127 to 127 (Byte.MIN_VAUE = NULL)
	// FCT SHORT  = byte code 2; float fact; max data length = 2; -32767 to 32767 (Short.MIN_VALUE = NULL)
	// FCT INTEGER  = byte code 3; integer fact; max data length = 4; -2^32 + 1 TO 2^31 -1 (Integer.MIN_VALUE = NULL)
	// FCT LONG  = byte code 4; Long fact; max data length = 8; -2^64 + 1 TO 2^63 -1 (Long.MIN_VALUE = NULL)
	// FCT FLOAT  = byte code 5; float fact; max data length = 4; (Float.MIN_VALUE = NULL)
	// FCT DOUBLE = byte code 6 double fact; max data length = 8; (Double.MIN_VALUE = NULL)
	// FCT DECIMAL CUSTOM = byte code 7; user defined data length and decimal position
	// FCT ALPHAN = byte code 8; alpha fact; MAX LENGTH = 40; (All Bytes = 0 = NULL)
	
	// FOR UNARY DATASET // position ids as integers
	// OLAP = byte code 21; unary keys as integers
	// DOC = byte code 22; unary keys as integers + preceeding shorts as message Type and Succeeding shorts as counts
	
	// FOR LINK DATASETS
	// UNCHAINED = byte code 11; a series of integers representing the to cluster id values
	// CHAINED = byte code 12; a series of chained integers
	
	// positions 2-5 = data length; expreseed in each position as a byte
	// E.g., length 2000 = byte code 2, byte code 0, byte code 0, byte code 0
	// E.g., length 20 = byte code 0, byte code 0, byte code 2, byte code 0
	
	// positions 6-7 = decimal length; expressed in each position as a byte 
	// E.g., decimal length 2 = byte code 0, byte code 2
	// E.g., decimal length 10 = byte code 1, byte code 0
	
	// position 8 - 17 = file name - represented as a sum of all character integer values
	//
	// position 18 - 36 = record count - represented as raw long
	// calculated by the conversion rule (file size - 51)/data length in position 1
	//
	// position 37 - 55 = time stamp - represented by raw long
	
	// constants associated with check sum
	public static final int FILE_CHECKSUM_LENGTH = 60;
	public static final int UNIFILE_CHECKSUM_LENGTH = 64;
	public static final int FILE_TYPE_POS = 0;
	public static final int FILE_ENCODING_TYPE_POS = 1;
	public static final int FILE_SEGMENT_NO_POS = 2;
	public static final int FILE_DATA_LEN_POS = 6;
	public static final int FILE_DECIMAL_SIZE_POS = 10;
	public static final int FILE_DATASET_NAME_POS = 12;
	public static final int FILE_DATASET_NAME_LEN = 10;
	public static final int FILE_DATASET_SIZE_POS = 22;
	public static final int FILE_DATASET_SIZE_LEN = 19;
	public static final int FILE_DATASET_TIME_POS = 41;
	public static final int FILE_DATASET_TIME_LEN = 19;
	
	// file types
	public static final int FILE_TYPE_DIM = 1;
	public static final int FILE_TYPE_FACT = 2;
	public static final int FILE_TYPE_UNI = 3;
	public static final int FILE_TYPE_LINK = 4;
	
	// dim encoding
	public static final int DIM_ENCODE_TYPE1 = 1;
	public static final int DIM_ENCODE_TYPE2 = 2;
	public static final int DIM_ENCODE_TYPE3 = 3;
	
	// fact encoding
	public static final int FACT_ENCODE_TYPE_BYTE = 1;
	public static final int FACT_ENCODE_TYPE_SHORT = 2;
	public static final int FACT_ENCODE_TYPE_INT = 3;
	public static final int FACT_ENCODE_TYPE_LONG = 4;
	public static final int FACT_ENCODE_TYPE_FLOAT = 5;
	public static final int FACT_ENCODE_TYPE_DOUBLE = 6;
	public static final int FACT_ENCODE_TYPE_DECIMAL = 7;
	public static final int FACT_ENCODE_TYPE_ALPHAN = 8;
	
	// uni encoding
	public static final int UNI_ENCODE_TYPE_OLAP = 21; // Record ids only
	public static final int UNI_ENCODE_TYPE_DOC = 22; // Document with counts
	
	// link encoding
	public static final int LINK_ENCODE_TYPE_UNCHAINED = 11; // Record ids only
	public static final int LINK_ENCODE_TYPE_CHAINED = 12; // Document with counts
		
	// Data lengths
	public static final int BYTE_LEN = 1;
	public static final int SHORT_LEN = 2;
	public static final int INT_FLOAT_LEN = 4;
	public static final int LONG_DOUBLE_LEN = 8;
	public static final int FACT_MAX_ALPHAN_LENGTH = 40;
	
	public CheckSum() {
		// TODO Auto-generated constructor stub
	}
	
	public static int computeDS(String name) {
		if (name == null || name.length() == 0)
			return 0;
		
		return Float.floatToIntBits(name.chars().sum());
	}
	
	public static long computeTFS(long value) {
		return Double.doubleToLongBits(value);
	}
	
	public static byte[] computeCS(String name, long filesize) {
		
		//System.out.println("DS : " + Integer.toString(computeDS(name)));
		//System.out.println("FS : " + Long.toString(computeTFS(filesize)));
		//System.out.println("TS : " + Long.toString(computeTFS(System.currentTimeMillis())));
		
		return (Integer.toString(computeDS(name)) + 
				Long.toString(computeTFS(filesize))).getBytes();
	}
	
	public static int validateFileforTruncate(String dbName, String datasetName, int fileType[], int[] encoding, int[] datasize, short[] decimals, int[] segmentNo) throws Exception {
		File f = new File(datasetName);
		int actualFileName = computeDS(dbName + "|" + datasetName);
		long actualFileSize = computeTFS(f.length());
		long actualLastModifiedTime = computeTFS(f.lastModified());
		
		if (fileType == null || fileType.length <= 0)
			fileType = new int[1];
		if (encoding == null || encoding.length <= 0)
			encoding = new int[1];
		if (datasize == null || datasize.length <= 0)
			datasize = new int[1];
		if (decimals == null || decimals.length <= 0)
			decimals = new short[1];
		if (segmentNo == null || segmentNo.length <= 0)
			segmentNo = new int[1];
		
		FileInputStream fis = new FileInputStream(f);
		BufferedInputStream buffer = new BufferedInputStream(fis);
		
		
		//RandomAccessFile raf = new RandomAccessFile(datasetName, "r");
		//FileChannel fc = raf.getChannel();
		//MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, FILE_CHECKSUM_LENGTH);
		
		// get the file type
		fileType[0] = buffer.read();
		encoding[0] = buffer.read();
		
		//datasize[0] = (buffer.read() * 1000) + (buffer.read() * 100) + (buffer.read() * 10) + buffer.read();
		
		//decimals[0] = (buffer.read() * 10) + buffer.read();
		byte[] intBytes = new byte[4];
		intBytes[0] = (byte)buffer.read();
		intBytes[1] = (byte)buffer.read();
		intBytes[2] = (byte)buffer.read();
		intBytes[3] = (byte)buffer.read();
		segmentNo[0] = ByteBuffer.wrap(intBytes).getInt();
		
		intBytes[0] = (byte)buffer.read();
		intBytes[1] = (byte)buffer.read();
		intBytes[2] = (byte)buffer.read();
		intBytes[3] = (byte)buffer.read();
		datasize[0] = ByteBuffer.wrap(intBytes).getInt();
		
		byte[] shortBytes = new byte[2];
		shortBytes[0] = (byte)buffer.read();
		shortBytes[1] = (byte)buffer.read();
		decimals[0] = ByteBuffer.wrap(shortBytes).getShort();
		
		//decimals[0] = (buffer.get() * 10) + buffer.get();
		
		
		//System.out.println("File type = " + fileType[0]);
		//System.out.println("File Encoding = " + encoding[0]);
		//System.out.println("File Data Length = " + datasize[0]);
		//System.out.println("File Decimals = " + decimals[0]);
		
		byte[] readFilenameByte = new byte[10];
		byte[] readFileSizeByte = new byte[19];
		byte[] readLastModifiedTimeByte = new byte[19];
		
		buffer.read(readFilenameByte);
		buffer.read(readFileSizeByte);
		buffer.read(readLastModifiedTimeByte);
		
		buffer.close();
		fis.close();
		
		buffer = null;
		
		//fc.close();
		//raf.close();
		
		//fc = null;
		//raf = null;
		
		int iReadFilename = Integer.parseInt(new String(readFilenameByte));
		long lReadFileSize = Long.parseLong(new String(readFileSizeByte));
		long lReadLastModifiedTime = Long.parseLong(new String(readLastModifiedTimeByte));
		
		//System.out.println("Dataset Name (actual/read): " + actualFileName + " / " + iReadFilename);
		//System.out.println("File size (actual/read): " + actualFileSize + " / " + lReadFileSize);
		//System.out.println("Last modified Time (actual/read): " + actualLastModifiedTime + " / " + lReadLastModifiedTime);
		
		if (actualFileName != iReadFilename || 
        		actualFileSize != lReadFileSize || 
        		actualLastModifiedTime != lReadLastModifiedTime)
        	throw new Exception ("File content changed by external process to HASIDS");
		
		return HASIDSConstants.SUCCESS;		
	}
	
	public static int validateFile(String dbName, String datasetName, int fileType[], int[] encoding, int[] datasize, short[] decimals, int[] segmentNo) throws Exception {
		File f = new File(datasetName);
		int actualFileName = computeDS(dbName + "|" + datasetName);
		long actualFileSize = computeTFS(f.length());
		long actualLastModifiedTime = computeTFS(f.lastModified());
		
		if (fileType == null || fileType.length <= 0)
			fileType = new int[1];
		if (encoding == null || encoding.length <= 0)
			encoding = new int[1];
		if (datasize == null || datasize.length <= 0)
			datasize = new int[1];
		if (decimals == null || decimals.length <= 0)
			decimals = new short[1];
		if (segmentNo == null || segmentNo.length <= 0)
			segmentNo = new int[1];
		
		RandomAccessFile raf = new RandomAccessFile(datasetName, "r");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, FILE_CHECKSUM_LENGTH);
		
		// get the file type
		fileType[0] = buffer.get();
		encoding[0] = buffer.get();
		
		//datasize[0] = (buffer.get() * 1000) + (buffer.get() * 100) + (buffer.get() * 10) + buffer.get();
		segmentNo[0] = buffer.getInt();
		datasize[0] = buffer.getInt();
		
		//decimals[0] = (buffer.get() * 10) + buffer.get();
		decimals[0] = buffer.getShort();
		//System.out.println("File type = " + fileType[0]);
		//System.out.println("File Encoding = " + encoding[0]);
		//System.out.println("File Data Length = " + datasize[0]);
		//System.out.println("File Decimals = " + decimals[0]);
		
		byte[] readFilenameByte = new byte[10];
		byte[] readFileSizeByte = new byte[19];
		byte[] readLastModifiedTimeByte = new byte[19];
		
		buffer.get(readFilenameByte);
		buffer.get(readFileSizeByte);
		buffer.get(readLastModifiedTimeByte);
		
		
		buffer = null;
		
		fc.close();
		raf.close();
		
		fc = null;
		raf = null;
		
		int iReadFilename = Integer.parseInt(new String(readFilenameByte));
		long lReadFileSize = Long.parseLong(new String(readFileSizeByte));
		long lReadLastModifiedTime = Long.parseLong(new String(readLastModifiedTimeByte));
		
		//System.out.println("Dataset Name (actual/read): " + actualFileName + " / " + iReadFilename);
		//System.out.println("File size (actual/read): " + actualFileSize + " / " + lReadFileSize);
		//System.out.println("Last modified Time (actual/read): " + actualLastModifiedTime + " / " + lReadLastModifiedTime);
		
		if (actualFileName != iReadFilename || 
        		actualFileSize != lReadFileSize || 
        		actualLastModifiedTime != lReadLastModifiedTime)
        	throw new Exception ("File content changed by external process to HASIDS");
		
		return HASIDSConstants.SUCCESS;		
	}
	
	
	public static int validateFile(String dbName, String datasetName) throws Exception {
		
		// send in nulls for the encoding
		return CheckSum.validateFile(dbName, datasetName, null, null, null, null, null);
	}

	
	public static int validateUNIFile(String dbName, String datasetName, int fileType[], int[] encoding, int[] datasize, short[] decimals, int[] segmentNo, int[] segmentCount) throws Exception {
		short messageTypeSize = 0;
		
		File f = new File(datasetName);
		long fileLength = f.length();
		
		//System.out.println("File Length : " + fileLength);
		
		int actualFileName = computeDS(dbName + "|" + datasetName);
		long actualFileSize = computeTFS(fileLength);
		long actualLastModifiedTime = computeTFS(f.lastModified());
		
		if (fileType == null || fileType.length <= 0)
			fileType = new int[1];
		if (encoding == null || encoding.length <= 0)
			encoding = new int[1];
		if (datasize == null || datasize.length <= 0)
			datasize = new int[1];
		if (decimals == null || decimals.length <= 0)
			decimals = new short[1];
		if (segmentCount == null || segmentCount.length <= 0)
			segmentCount = new int[1];
		
		RandomAccessFile raf = new RandomAccessFile(datasetName, "r");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, UNIFILE_CHECKSUM_LENGTH);
		
		// get the file type
		fileType[0] = buffer.get();
		encoding[0] = buffer.get();
		segmentNo[0] = buffer.getInt();
		datasize[0] = buffer.getInt();
		decimals[0] = buffer.getShort();
		
		/*System.out.println("File type = " + fileType[0]);
		System.out.println("File Encoding = " + encoding[0]);
		System.out.println("Segment No = " + segmentNo[0]);
		System.out.println("File Data Size = " + datasize[0]);
		System.out.println("File Decimals = " + decimals[0]);
		System.out.println("Buffer position : " + buffer.position());*/
		byte[] readFilenameByte = new byte[10];
		byte[] readFileSizeByte = new byte[19];
		byte[] readLastModifiedTimeByte = new byte[19];
		
		buffer.get(readFilenameByte);
		//System.out.println("Buffer position : " + buffer.position());
		buffer.get(readFileSizeByte);
		//System.out.println("Buffer position : " + buffer.position());
		buffer.get(readLastModifiedTimeByte);
		//System.out.println("Buffer position : " + buffer.position());
		
		
		// get the segment size allocated for this file
		segmentCount[0] = buffer.getInt();
		//System.out.println("Segment Count : " + segmentCount[0]);
		//System.out.println("Buffer position : " + buffer.position());
		// compute the actual record id count in the file
		int actualRecordCount = (int)(fileLength - CheckSum.UNIFILE_CHECKSUM_LENGTH)/(datasize[0] + decimals[0]);
		//System.out.println("Actual record count : " + actualRecordCount);
		
		if (encoding[0] == CheckSum.UNI_ENCODE_TYPE_DOC)
			messageTypeSize = 2;
		
		// compute any spillover
		int spillover = (int)(fileLength - CheckSum.UNIFILE_CHECKSUM_LENGTH)%(datasize[0] + decimals[0] + messageTypeSize);
		//System.out.println("Spillover : " + spillover);
		buffer = null;
		fc.close();
		raf.close();
		
		// check if the actual number of record ids are greater > 
		if (actualRecordCount > segmentCount[0] || spillover > 0)
			throw new Exception ("File content changed by external process to HASIDS");
		
		int iReadFilename = Integer.parseInt(new String(readFilenameByte));
		long lReadFileSize = Long.parseLong(new String(readFileSizeByte));
		long lReadLastModifiedTime = Long.parseLong(new String(readLastModifiedTimeByte));
		
		//System.out.println("Dataset Name (actual/read): " + actualFileName + " / " + iReadFilename);
		//System.out.println("File size (actual/read): " + actualFileSize + " / " + lReadFileSize);
		//System.out.println("Last modified Time (actual/read): " + actualLastModifiedTime + " / " + lReadLastModifiedTime);
		
		if (actualFileName != iReadFilename || 
        		actualFileSize != lReadFileSize || 
        		actualLastModifiedTime != lReadLastModifiedTime)
        	throw new Exception ("File content changed by external process to HASIDS");
		
		return HASIDSConstants.SUCCESS;		
	}
	
	
	public static int validateUNIFile(String dbName, String datasetName) throws Exception {
		
		// send in nulls for the encoding
		return CheckSum.validateUNIFile(dbName, datasetName, null, null, null, null, null, null);
	}
}
