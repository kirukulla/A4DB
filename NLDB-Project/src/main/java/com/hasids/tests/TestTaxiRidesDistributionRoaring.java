package com.hasids.tests;

import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;

import com.hasids.io.dim.DimDataReader;

public class TestTaxiRidesDistributionRoaring {

	public TestTaxiRidesDistributionRoaring() {
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
			RoaringBitmap[] bArrayCabTypeSeg1 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_2.DM";
			RoaringBitmap[] bArrayCabTypeSeg2 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_3.DM";
			RoaringBitmap[] bArrayCabTypeSeg3 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_4.DM";
			RoaringBitmap[] bArrayCabTypeSeg4 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
							
							
			for(int i = 1; i <= noDistributions; i++) {
				System.out.println("Segment : 0, Cardinality for " + i + " = " + bArrayCabTypeSeg1[i].getCardinality());
				System.out.println("Segment : 1, Cardinality for " + i + " = " + bArrayCabTypeSeg2[i].getCardinality());
				System.out.println("Segment : 2, Cardinality for " + i + " = " + bArrayCabTypeSeg3[i].getCardinality());
				System.out.println("Segment : 3, Cardinality for " + i + " = " + bArrayCabTypeSeg4[i].getCardinality());
			}
			
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time to read cab type distributions into BitSets : " + diff);				
							
			beginTime = System.nanoTime();
			
			System.out.println("Ride year distribution");
			noDistributions = 3;
							
			beginTime = System.nanoTime();
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_1.DM";
							
			RoaringBitmap[] bArrayTripYearSeg1 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_2.DM";
			RoaringBitmap[] bArrayTripYearSeg2 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_3.DM";
			RoaringBitmap[] bArrayTripYearSeg3 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_4.DM";
			RoaringBitmap[] bArrayTripYearSeg4 = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributions);
							
			for(int i = 1; i <= noDistributions; i++) {
				System.out.println("Segment : 0, Cardinality for " + i + " = " + bArrayTripYearSeg1[i].getCardinality());
				System.out.println("Segment : 1, Cardinality for " + i + " = " + bArrayTripYearSeg2[i].getCardinality());
				System.out.println("Segment : 2, Cardinality for " + i + " = " + bArrayTripYearSeg3[i].getCardinality());
				System.out.println("Segment : 3, Cardinality for " + i + " = " + bArrayTripYearSeg4[i].getCardinality());
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time to read pickup year distributions into RoaringBitmaps : " + diff);				
			
			System.out.println("BEGIN GROUPING............");
			beginTime = System.nanoTime();
			int cabTypes = bArrayCabTypeSeg1.length - 1;
			int rideYears = bArrayTripYearSeg1.length - 1;
			long sum = 0;
			RoaringBitmap b1, b2, b3, b4;
			for(int i = 1; i <= cabTypes; i++) {
				for (int j = 1; j <= rideYears; j++) {
					sum = 0;
					if (bArrayCabTypeSeg1[i].getCardinality() > 0) {
						if (bArrayTripYearSeg1[j].getCardinality() > 0) {
							b1 = bArrayCabTypeSeg1[i].clone();
							b1.and(bArrayTripYearSeg1[j]);
							sum += b1.getCardinality();
						}
					}
					
					if (bArrayCabTypeSeg2[i].getCardinality() > 0) {
						if (bArrayTripYearSeg2[j].getCardinality() > 0) {
							b2 = bArrayCabTypeSeg2[i].clone();
							b2.and(bArrayTripYearSeg2[j]);
							sum += b2.getCardinality();
						}
					}
					
					if (bArrayCabTypeSeg1[i].getCardinality() > 0) {
						if (bArrayTripYearSeg3[j].getCardinality() > 0) {
							b3 = bArrayCabTypeSeg3[i].clone();
							b3.and(bArrayTripYearSeg3[j]);
							sum += b3.getCardinality();
						}
					}
					
					if (bArrayCabTypeSeg4[i].getCardinality() > 0) {
						if (bArrayTripYearSeg4[j].getCardinality() > 0) {
							b4 = bArrayCabTypeSeg4[i].clone();
							b4.and(bArrayTripYearSeg4[j]);
							sum += b4.getCardinality();
						}
					}
					
					System.out.println("CAB TYPE : " + i + " Year : " + j + ", Count = " + sum);
				}
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("GROUPING count calculation in memory in millis (Bitmap) : " + diff);
			
		}
		catch (Exception e) {
							
		}
	}

}
