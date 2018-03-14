package com.hasids.tests;

import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;

import com.hasids.io.dim.DimDataReader;

public class TestDimGrouping {

	public TestDimGrouping() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// GROUPING EXAMPLE BETWEEN DAY OF MONTH AND SEX USING BITMAPS
		try {
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			int noDistributionsDay = 31;
					
			long beginTime = System.nanoTime();
					
			BitSet[] bArrayDay = DimDataReader.getDataDistributions(dbName, datasetName, noDistributionsDay);
					
					
					
					
			int noDistributionsSex = 2;
					
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
					
			BitSet[] bArraySex = DimDataReader.getDataDistributions(dbName, datasetName, noDistributionsSex);
					
			for(int i = 1; i <= noDistributionsDay; i++)
				System.out.println("Cardinality for " + i + " = " + bArrayDay[i].cardinality());
					
					
			for(int i = 1; i <= noDistributionsSex; i++)
				System.out.println("Cardinality for " + i + " = " + bArraySex[i].cardinality());
					
					
			int dayCount = bArrayDay.length;
			int sexCount = bArraySex.length;
					
			System.out.println("Day distribution size : " + dayCount);
			System.out.println("Sex distribution size : " + sexCount);
					
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time to retrieve data into memory in millis (BitSet) : " + diff);
					
			beginTime = System.nanoTime();
			String newLine = "" + '\n';
			String header = "DAY OF MONTH" + '\t' + "MALES" + '\t' + "FEMALES";
			StringBuffer sb = new StringBuffer();
					
			for (int i = 1; i < dayCount; i++) {
				sb.append(i);
				for(int j = 1; j < sexCount; j++) {
					BitSet b = (BitSet) bArrayDay[i].clone();
					BitSet c = bArraySex[j];
					b.and(c);
					sb.append('\t').append('\t').append(b.cardinality());
					//System.out.println("Grouping count for day : " + i + " and sex : " + j + " = " + b.cardinality());
				}
				sb.append(newLine);
			}
					
			System.out.println(header);
			System.out.println(sb.toString());
					
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Grouping time in memory in millis (BitSet) : " + diff);
					
					
					
					
				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
				
				
		// GROUPING EXAMPLE BETWEEN DAY OF MONTH AND SEX USING ROARING BITMAPS
		try {
			String dbName = "Test";
			String datasetName = "c:\\users\\dpras\\tempdata\\testdata\\dayofmonth_1.DM";
			int noDistributionsDay = 31;
					
			long beginTime = System.nanoTime();
					
			RoaringBitmap[] bArrayDay = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributionsDay);
					
					
			int noDistributionsSex = 2;
					
			datasetName = "c:\\users\\dpras\\tempdata\\testdata\\sex_1.DM";
					
			RoaringBitmap[] bArraySex = DimDataReader.getDataDistributionsRoaring(dbName, datasetName, noDistributionsSex);
					
			for(int i = 1; i <= noDistributionsDay; i++) {
				System.out.println("Cardinality for " + i + " = " + bArrayDay[i].getCardinality() + ", Size : " + bArrayDay[i].getSizeInBytes()/1024/1024 + " Mb");
			}
					
			for(int i = 1; i <= noDistributionsSex; i++)
				System.out.println("Cardinality for " + i + " = " + bArraySex[i].getCardinality() + ", Size : " + bArraySex[i].getSizeInBytes()/1024/1024 + " Mb");
					
					
			int dayCount = bArrayDay.length;
			int sexCount = bArraySex.length;
					
			System.out.println("Day distribution size : " + dayCount);
			System.out.println("Sex distribution size : " + sexCount);
					
			long endTime = System.nanoTime();
			long diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Time to retrieve data into memory in millis (BitSet) : " + diff);
					
			beginTime = System.nanoTime();
			String newLine = "" + '\n';
			String header = "DAY OF MONTH" + '\t' + "MALES" + '\t' + "FEMALES";
			StringBuffer sb = new StringBuffer();
					
			for (int i = 1; i < dayCount; i++) {
				sb.append(i);
				for(int j = 1; j < sexCount; j++) {
					RoaringBitmap b = (RoaringBitmap) bArrayDay[i].clone();
					RoaringBitmap c = bArraySex[j];
					b.and(c);
					sb.append('\t').append('\t').append(b.getCardinality());
					//System.out.println("Grouping count for day : " + i + " and sex : " + j + " = " + b.cardinality());
				}
				sb.append(newLine);
			}
					
			System.out.println(header);
			System.out.println(sb.toString());
					
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Grouping time in memory in millis (BitSet) : " + diff);					
					
				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}

}
