package com.hasids.tests;

import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;

import com.hasids.io.dim.DimDataReader;

public class TestTaxiRidesDistribution {

	public TestTaxiRidesDistribution() {
		// TODO Auto-generated constructor stub
						
	}
	
	public static void main(String[] args) {
		// DISTRIBUTIONS EXAMPLE
		
		try {
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_1.DM";
			int noDistributions = 4;
					
			long beginTime = System.nanoTime();
					
			System.out.println("Cab types distribution");
			BitSet[] bArrayCabTypeSeg1 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_2.DM";
			BitSet[] bArrayCabTypeSeg2 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_3.DM";
			BitSet[] bArrayCabTypeSeg3 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_4.DM";
			BitSet[] bArrayCabTypeSeg4 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
							
							
							
			for(int i = 1; i <= noDistributions; i++) {
				System.out.println("Segment : 0, Cardinality for " + i + " = " + bArrayCabTypeSeg1[i].cardinality());
				System.out.println("Segment : 1, Cardinality for " + i + " = " + bArrayCabTypeSeg2[i].cardinality());
				System.out.println("Segment : 2, Cardinality for " + i + " = " + bArrayCabTypeSeg3[i].cardinality());
				System.out.println("Segment : 3, Cardinality for " + i + " = " + bArrayCabTypeSeg4[i].cardinality());
			}
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (BitSet) : " + diff);
							
			beginTime = System.nanoTime();
			
			System.out.println("Ride year distribution");
			noDistributions = 3;
							
			beginTime = System.nanoTime();
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_1.DM";
							
			BitSet[] bArrayTripYearSeg1 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_2.DM";
			BitSet[] bArrayTripYearSeg2 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_3.DM";
			BitSet[] bArrayTripYearSeg3 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_4.DM";
			BitSet[] bArrayTripYearSeg4 = DimDataReader.getDataDistributions(dbName, datasetName, noDistributions);
							
			for(int i = 1; i <= noDistributions; i++) {
				System.out.println("Segment : 0, Cardinality for " + i + " = " + bArrayTripYearSeg1[i].cardinality());
				System.out.println("Segment : 1, Cardinality for " + i + " = " + bArrayTripYearSeg2[i].cardinality());
				System.out.println("Segment : 2, Cardinality for " + i + " = " + bArrayTripYearSeg3[i].cardinality());
				System.out.println("Segment : 3, Cardinality for " + i + " = " + bArrayTripYearSeg4[i].cardinality());
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time in memory in millis (BitSet) : " + diff);				
			
			System.out.println("BEGIN GROUPING............");
			beginTime = System.nanoTime();
			int cabTypes = bArrayCabTypeSeg1.length - 1;
			int rideYears = bArrayTripYearSeg1.length - 1;
			long sum = 0;
			BitSet b1, b2, b3, b4;
			for(int i = 1; i <= cabTypes; i++) {
				for (int j = 1; j <= rideYears; j++) {
					sum = 0;
					if (bArrayCabTypeSeg1[i].cardinality() > 0) {
						if (bArrayTripYearSeg1[j].cardinality() > 0) {
							b1 = (BitSet)bArrayCabTypeSeg1[i].clone();
							b1.and(bArrayTripYearSeg1[j]);
							sum += b1.cardinality();
						}
					}
					
					if (bArrayCabTypeSeg2[i].cardinality() > 0) {
						if (bArrayTripYearSeg2[j].cardinality() > 0) {
							b2 = (BitSet)bArrayCabTypeSeg2[i].clone();
							b2.and(bArrayTripYearSeg2[j]);
							sum += b2.cardinality();
						}
					}
					
					if (bArrayCabTypeSeg1[i].cardinality() > 0) {
						if (bArrayTripYearSeg3[j].cardinality() > 0) {
							b3 = (BitSet)bArrayCabTypeSeg3[i].clone();
							b3.and(bArrayTripYearSeg3[j]);
							sum += b3.cardinality();
						}
					}
					
					if (bArrayCabTypeSeg4[i].cardinality() > 0) {
						if (bArrayTripYearSeg4[j].cardinality() > 0) {
							b4 = (BitSet)bArrayCabTypeSeg4[i].clone();
							b4.and(bArrayTripYearSeg4[j]);
							sum += b4.cardinality();
						}
					}
					
					System.out.println("CAB TYPE : " + i + " Year : " + j + ", Count = " + sum);
				}
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Operations time of grouping count calculation in memory in millis (Bitmap) : " + diff);
			
		}
		catch (Exception e) {
							
		}
	}

}
