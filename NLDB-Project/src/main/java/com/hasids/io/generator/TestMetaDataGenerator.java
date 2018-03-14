/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids.io.generator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;


public class TestMetaDataGenerator {
	
	/**
	 * Default no arguments Constructor 
	 * 
	 * 
	 */
	public TestMetaDataGenerator() {
		super();
	}
	
	
	
	private Hashtable<String, Hashtable<Integer, int[]>> getMetadataInfo (String[] filenames) throws Exception {
		
		Hashtable<String, Hashtable<Integer, int[]>> h = new Hashtable<String, Hashtable<Integer, int[]>>();
		// track the beginning time of the job
		long beginTime = System.nanoTime();
		
		
        try {
        	// reset counters
        	
        	for (int i = 0; i < filenames.length; i++) {
        		// Check if file exists
        		File f = new File(filenames[i]);
        		if (!f.exists())
        			continue;
        		
        		// open file and map to memory
        		RandomAccessFile aFile = new RandomAccessFile(filenames[i], "r");
        		long fileSize = aFile.length();
        		
        		// get a handle to the file channel
        		FileChannel inChannel = aFile.getChannel();
            
        		// buffer the mapped data for read
        		MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        		int read;
        		Integer I = null;
        		Hashtable<Integer, int[]> tempTable = null;
        		// read byte by byte, check the byte value for existence in a collecting
        		// hashtable and build the counts, low and high ranges
        		for (int j = 0; j < buffer.limit(); j++) {
            		
            		// read each character byte
            		read = buffer.get();
            		I = new Integer(read);
            		
            		// get the hashtable associated with the file
            		// if no file found, create an entry
            		tempTable = h.get(filenames[i]);
            		if (tempTable == null) {
            			// create the byte holder as an array of three integers
            			// the first is low value, second is high value and the third is count
            			int k[] = new int[] {j, j, 1};
            			tempTable = new Hashtable<Integer, int[]>();
            			tempTable.put(I, k);
            			h.put(filenames[i], tempTable);
            		}
            		else {
            			//System.out.println("position : " + j);
            			int k[] = (int[]) tempTable.get(I);
            			if (k == null) {
            				k = new int[] {j, j, 1};
            			}
            			else {
            				// reset the low, high and increment the count counter
            				if (k[0] > j) k[0] = j;
            				if (k[1] < j) k[1] = j;
            				k[2]++;
            			}
            			
            			// put the byte counters back into the hashtable
            			tempTable.put(I, k);
            		}
            		
            	}
            
        		// close the channel
        		inChannel.close();
        		// close the file
        		aFile.close();
            
        		// clear the buffer
        		buffer.clear();
        		buffer = null;
        		
        	}    
        	
        	long endTime = System.nanoTime();
        	System.out.println("Run time : " + TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS) + " millis");
            
        	
        } catch (IOException ioe) {
            throw new IOException(ioe);
        } finally {
        	
        	;
        }
        
        return h;
        
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String f1 = "c:\\users\\dpras\\tempdata\\dayofmonth";
		String f2 = "c:\\users\\dpras\\tempdata\\month";
		String f3 = "c:\\users\\dpras\\tempdata\\year";
		String f4 = "c:\\users\\dpras\\tempdata\\sex";
		String f5 = "c:\\users\\dpras\\tempdata\\race";
		String f6 = "c:\\users\\dpras\\tempdata\\type";
		
		String[] filenames = {f1, f2, f3, f4, f5, f6};
		
		try {
			TestMetaDataGenerator tm = new TestMetaDataGenerator();
			Hashtable<String, Hashtable<Integer, int[]>> h = tm.getMetadataInfo(filenames);
			if (h == null) {
				System.out.println("No data returned");
				System.exit(1);
			}
			
			Enumeration<String> e = h.keys();
			while (e.hasMoreElements()) {
				String filename = e.nextElement();
				System.out.println(filename);
				Hashtable<Integer, int[]> tempTable = h.get(filename);
				Enumeration<Integer> et = tempTable.keys();
				while (et.hasMoreElements()) {
					Integer I = et.nextElement();
					int k[] = (int[]) tempTable.get(I);
					System.out.println("Key : " + I.intValue() + ", low = " + (k[0] + 1) + ", high = " + (k[1] + 1) + ", count = " + k[2]);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}


}

