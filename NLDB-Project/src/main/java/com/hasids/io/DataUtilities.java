package com.hasids.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.hasids.datastructures.CheckSum;

public class DataUtilities {

	public DataUtilities() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Method to truncate a dataset to empty and reset the headers
	 * @param dbName
	 * @param datasetName
	 * @throws Exception
	 */
	public static void truncate(String dbName, String datasetName) throws Exception {

		// TODO Auto-generated constructor stub
		if (dbName == null || dbName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		if (datasetName == null || datasetName.trim().length() <= 0)
			throw new Exception ("Invalid dbName!");
		
		try {
			File f = new File(datasetName);
			if (!f.exists())
				throw new Exception("File " + datasetName + " does not exist!");
			
			int[] fileType = new int[1];
			int[] encoding = new int[1];
			int[] segmentNo = new int[1];
			int[] datasize = new int[1];
			short[] decimals = new short[1];
			
			CheckSum.validateFileforTruncate(dbName, datasetName, fileType, encoding, datasize, decimals, segmentNo);
			
			if (fileType[0] != CheckSum.FILE_TYPE_DIM && fileType[0] != CheckSum.FILE_TYPE_FACT && 
					fileType[0] != CheckSum.FILE_TYPE_UNI)
				throw new Exception("Invalid file type!");
			
			int recordLength = datasize[0];
			if (fileType[0] == CheckSum.FILE_TYPE_UNI)
				recordLength = recordLength + decimals[0] + 2;
			
			// set the map size to the original file length
			int mapSize = (int)f.length();
			// reset the size to header if file type is UNI 
			if (fileType[0] == CheckSum.FILE_TYPE_UNI)
				mapSize = CheckSum.UNIFILE_CHECKSUM_LENGTH;
			
			// open the file
			RandomAccessFile randomAccessFile = new RandomAccessFile(datasetName, "rw");
			// get the file channel
			FileChannel rwChannel = randomAccessFile.getChannel();
			// lock the channel
			FileLock fl = rwChannel.lock();
			
			// truncate the file up to end of file header 
			if (fileType[0] == CheckSum.FILE_TYPE_UNI)
				rwChannel.truncate(CheckSum.UNIFILE_CHECKSUM_LENGTH); 
			else
				rwChannel.truncate(CheckSum.FILE_CHECKSUM_LENGTH);
			
			// map the file into memory, resetting the size
			MappedByteBuffer buffer = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
			
			// set the position to filename and modify the filename and size
			if (fileType[0] == CheckSum.FILE_TYPE_UNI) {
				buffer.position(CheckSum.FILE_DATASET_NAME_POS);
				buffer.put(CheckSum.computeCS(dbName + "|" + datasetName, CheckSum.UNIFILE_CHECKSUM_LENGTH));
			}
			
			// get the last modified time
			long lastModifiedTime = System.currentTimeMillis();
			// set the buffer to the timestamp position
			buffer.position(CheckSum.FILE_DATASET_TIME_POS);
			// Add it to the buffer
			buffer.put(Long.toString(CheckSum.computeTFS(lastModifiedTime)).getBytes());
			// force the buffer
			buffer.force();
			// release buffer to GC
			buffer = null;
			// ask GC to collect the released buffer
			System.gc();
			// release the lock
			fl.release();
			// close the channel
			rwChannel.close();
			// close the file
			randomAccessFile.close();
			// set the file last modified time
			f.setLastModified(lastModifiedTime);
			
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
	}
	
	public static float getComputedBytes(String input) throws Exception {
		if (input == null)
			throw new Exception ("Null string received as input!");
		
		byte[] b = input.getBytes();
		int len = b.length;
		int sum = 0;
		for (int i = 0; i < len; i++)
			sum = sum + (b[i] * (i + 1));
		
		return (sum + len/100);
	}
	
	public static void main(String[] args) {
		
		String dbName = "Test";
		String datasetName1 = "c:\\users\\dpras\\tempdata\\testCS1_1.DM";
		
		try {
			
			// truncate the file
			DataUtilities.truncate(dbName, datasetName1);
			
			
		}
		catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}
