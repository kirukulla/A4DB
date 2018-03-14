package com.hasids.tests;

import java.io.PrintWriter;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

public class TestBitSet {

	public TestBitSet() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// Test the setting of bitsets in blocks of 100000 across different positions
		// first in position 1 - 100000, then the same 100000 in positions 100001 to 200000 and so on
		
		String outputFile = "c:\\users\\dpras\\tempdata\\tests\\bitsettest.txt";
		
		try {
			//open the test file
			PrintWriter f = new PrintWriter(outputFile);
			int i = 0;
			int bitsetCount = 100000;
			int size = 2000000000;
			BitSet b = new BitSet (size);
		
			for (i = 0; i < 6; i++) {
				int start = 0;
				int end = (bitsetCount * (int)Math.pow(10, i)) - 1;
				if (i == 5)
					end = size - 1;
				
				long beginTime = System.nanoTime();
				
				for (int j = start; j <= end; j++) {
					b.set(j);
				}
				
				long endTime = System.nanoTime();
				long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
	            
				f.println((end - start + 1) + "," + diff);
			}
			
			f.flush();
			f.close();
		}
		catch (Exception e) {
			;
		}
		
	}

}
