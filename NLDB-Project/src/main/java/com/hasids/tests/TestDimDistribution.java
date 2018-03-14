package com.hasids.tests;

import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;

import com.hasids.io.dim.DimDataReader;

public class TestDimDistribution {

	public TestDimDistribution() {
		// TODO Auto-generated constructor stub
						
	}
	
	public static void main(String[] args) {
		// DISTRIBUTIONS EXAMPLE
		
		try {
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			int noDistributions = 31;
					
			long beginTime = System.nanoTime();
					
			System.out.println("Bitset test");
			BitSet[] bArray = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
							
							
							
			for(int i = 0; i <= noDistributions; i++)
				System.out.println("Cardinality for " + i + " = " + bArray[i].cardinality());
							
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (BitSet) : " + diff);
							
			beginTime = System.nanoTime();
			System.out.println("Roaring bitmap test");
						
			RoaringBitmap[] rbArray = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			for(int i = 0; i <= noDistributions; i++)
				System.out.println("Cardinality for " + i + " = " + rbArray[i].getCardinality() + ", Size in Bytes : " + rbArray[i].getSizeInBytes());
							
							
							
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (RoaringBitmap) : " + diff);
						
			beginTime = System.nanoTime();
							
			// converting roaring to bitset
			for(int i = 1; i <= noDistributions; i++) {
				RoaringBitmap rb = rbArray[i];
				BitSet b = new BitSet(500000000);
				Iterator<Integer> it = rb.iterator();
				while(it.hasNext())
					b.set(it.next());
								
				endTime = System.nanoTime();
				diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
				System.out.println("Operations time in memory in millis (To convert RoaringBitmaps " + i + " to BitSet) : " + diff);
				beginTime = System.nanoTime();
			}
							
			noDistributions = 2;
							
			beginTime = System.nanoTime();
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
							
			bArray = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
							
			for(int i = 1; i <= noDistributions; i++)
				System.out.println("Cardinality for " + i + " = " + bArray[i].cardinality());
							
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (BitSet) : " + diff);
						
			beginTime = System.nanoTime();
							
			rbArray = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			for(int i = 1; i <= noDistributions; i++)
				System.out.println("Cardinality for " + i + " = " + rbArray[i].getCardinality() + ", Size in Bytes : " + rbArray[i].getSizeInBytes());
							
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (RoaringBitmap) : " + diff);
							
							
							
						
		}
		catch (Exception e) {
							
		}
	}

}
