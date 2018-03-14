package com.hasids.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.hasids.HASIDSConstants;
import com.hasids.datastructures.DataOperations;
import com.hasids.io.dim.DimDataReader;

public class TestTaxiRidesDistributionImmutableRoaring {

	public TestTaxiRidesDistributionImmutableRoaring() {
		// TODO Auto-generated constructor stub
						
	}

	public static void main(String[] args) {
		// DISTRIBUTIONS EXAMPLE
		
		try {
			long beginTime = System.nanoTime();
			long endTime = 0L;
			long diff = 0L;
			
			
			String queryName = "Taxi distribution Query";
			String dbName = "Test";
			
			
			ThreadGroup tg = new ThreadGroup("Master" + queryName);
			String datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_1.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread1 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t1 = new Thread(tg, tReadThread1);
			t1.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_2.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread2 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t2 = new Thread(tg, tReadThread2);
			t2.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_3.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread3 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t3 = new Thread(tg, tReadThread3);
			t3.start();

			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\cabtype_4.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread4 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t4 = new Thread(tg, tReadThread4);
			t4.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_1.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread5 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t5 = new Thread(tg, tReadThread5);
			t5.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_2.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread6 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t6 = new Thread(tg, tReadThread6);
			t6.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_3.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread7 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t7 = new Thread(tg, tReadThread7);
			t7.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\tripyear_4.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread8 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t8 = new Thread(tg, tReadThread8);
			t8.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_1.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread9 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t9 = new Thread(tg, tReadThread9);
			t9.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_2.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread10 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t10 = new Thread(tg, tReadThread10);
			t10.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_3.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread11 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t11 = new Thread(tg, tReadThread11);
			t11.start();
			
			datasetName = "c:\\users\\dpras\\tempdata\\taxidata\\passcount_4.DM";
			
			TestTaxiRidesDistributionReadThread tReadThread12 = 
					new TestTaxiRidesDistributionReadThread(dbName, datasetName);
			Thread t12 = new Thread(tg, tReadThread12);
			t12.start();
			
			while(tg.activeCount() > 0)
				Thread.sleep(100);
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Distribution read of all segments (ImmutableRoaring) : " + diff);
			
			//----------------------------------------------------------------------------------
			// TEST 1
			beginTime = System.nanoTime();
			
			// get the count of cab types
			TestTaxiRidesDistributionReadThread[] cabTypes = 
					{tReadThread1, tReadThread2, tReadThread3, tReadThread4};
			
			TestTaxiSummation cabTypesSummation = new TestTaxiSummation(cabTypes);
			cabTypesSummation.run();
			Hashtable<Integer, Long> hCounts = cabTypesSummation.getSummation();
			Enumeration<Integer> e = hCounts.keys();
			while(e.hasMoreElements()) {
				int key = e.nextElement();
				System.out.println("CAB TYPE : " + key + ", " + hCounts.get(key));
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Cab type counts summation time : " + diff);
			
			//----------------------------------------------------------------------------------
			// Passenger count and year grouping
			// TEST 2
			beginTime = System.nanoTime();
			
			tg = new ThreadGroup(queryName);
			// segment 1
			TestTaxiRidesGrouping ttg1 = new TestTaxiRidesGrouping(tReadThread5, tReadThread9);
			Thread tg1 = new Thread(tg, ttg1);
			tg1.start();
			
			// segment 2
			TestTaxiRidesGrouping ttg2 = new TestTaxiRidesGrouping(tReadThread6, tReadThread10);
			Thread tg2 = new Thread(tg, ttg2);
			tg2.start();
						
			// segment 3
			TestTaxiRidesGrouping ttg3 = new TestTaxiRidesGrouping(tReadThread7, tReadThread11);
			Thread tg3 = new Thread(tg, ttg3);
			tg3.start();
						
			// segment 4
			TestTaxiRidesGrouping ttg4 = new TestTaxiRidesGrouping(tReadThread8, tReadThread12);
			Thread tg4 = new Thread(tg, ttg4);
			tg4.start();
			
			while(tg.activeCount() > 0)
				Thread.sleep(20);
			
			HashMap<String, Integer> hm1 = ttg1.getgroupingCounts();
			Set<String> s1 = hm1.keySet();
			Iterator<String> i1 = s1.iterator();
			
			HashMap<String, Integer> hm2 = ttg2.getgroupingCounts();
			Set<String> s2 = hm2.keySet();
			Iterator<String> i2 = s2.iterator();
			
			HashMap<String, Integer> hm3 = ttg3.getgroupingCounts();
			Set<String> s3 = hm3.keySet();
			Iterator<String> i3 = s3.iterator();
			
			HashMap<String, Integer> hm4 = ttg4.getgroupingCounts();
			Set<String> s4 = hm4.keySet();
			Iterator<String> i4 = s4.iterator();
			
			HashMap<String, Long> results = new HashMap<String, Long>();
			// get the counts from first group - seg 1
			while(i1.hasNext()) {
				String key = i1.next();
				Long sum = results.get(key);
				if (sum == null)
					sum = 0L;
				sum += hm1.get(key);
				results.put(key, sum);
			}
			
			// get the counts from first group - seg 2
			while(i2.hasNext()) {
				String key = i2.next();
				Long sum = results.get(key);
				if (sum == null)
					sum = 0L;
				sum += hm2.get(key);
				results.put(key, sum);
			}
			
			// get the counts from first group - seg 3
			while(i3.hasNext()) {
				String key = i3.next();
				Long sum = results.get(key);
				if (sum == null)
					sum = 0L;
				sum += hm3.get(key);
				results.put(key, sum);
			}			
			
			// get the counts from first group - seg 4
			while(i4.hasNext()) {
				String key = i4.next();
				Long sum = results.get(key);
				if (sum == null)
					sum = 0L;
				sum += hm4.get(key);
				results.put(key, sum);
			}
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Passenger count and Trip year grouping time : " + diff);

			Set<String> sResults = results.keySet();
			Iterator<String> iResults = sResults.iterator();
			while(iResults.hasNext()) {
				String key = iResults.next();
				System.out.println(key + " : " + results.get(key));
			}
			
			System.out.println("__________________________________________________");
			i1 = s1.iterator();
			while(i1.hasNext()) {
				String key = i1.next();
				System.out.println("Key : " + key + ", Count : " + hm1.get(key));
			}
			
			System.out.println("__________________________________________________");
			
			// Test again
			
			beginTime = System.nanoTime();
			
			ArrayList<Hashtable<Integer, ImmutableRoaringBitmap>> al = new ArrayList<Hashtable<Integer, ImmutableRoaringBitmap>>();
			al.add(tReadThread5.getDistributions());
			al.add(tReadThread9.getDistributions());
			
			DataOperations dop = new DataOperations(al, DataOperations.INTERSECTION);
			Thread t = new Thread(dop);
			t.start();
			
			while(t.isAlive())
				Thread.sleep(10);
			
			endTime = System.nanoTime();
			diff = TimeUnit.MILLISECONDS.convert((endTime - beginTime), TimeUnit.NANOSECONDS);
			System.out.println("Passenger count and Trip year grouping time 1 : " + diff);

			Hashtable<String, ImmutableRoaringBitmap> htemp = dop.getGroupingResults();
			Enumeration<String> etemp = htemp.keys();
			while(e.hasMoreElements()) {
				String key = etemp.nextElement();
				System.out.println("Key : " + key + ", Count : " + htemp.get(key).getCardinality());
			}
			
			
			
		}
		catch (Exception e) {
			e.printStackTrace();				
		}
	}

}



class TestTaxiRidesDistributionReadThread implements Runnable {
	
	String _dbName = null;
	String _datasetName = null;
	Hashtable<Integer, ImmutableRoaringBitmap> _h = null;
	int _status = HASIDSConstants.THREAD_INACTIVE;
	
	public TestTaxiRidesDistributionReadThread(String dbName, String datasetName) {
		this._dbName = dbName;
		this._datasetName = datasetName;
	}
	
	public int getStatus() {
		return this._status;
	}
	
	public Hashtable<Integer, ImmutableRoaringBitmap> getDistributions() {
		return this._h;
	}

	public void run() {
		//System.out.println("Begin thread : " + this._datasetName);
		_status = HASIDSConstants.THREAD_ACTIVE;
		this._h = DimDataReader.getDataDistributionsMutable(this._dbName, this._datasetName);
		if (this._h == null || this._h.size() <= 0)
			_status = HASIDSConstants.THREAD_FAILED;
		else
			System.out.println("No distribution (" + this._datasetName + ") : " + this._h.size());
	}
	
}

class TestTaxiSummation implements Runnable {
	private TestTaxiRidesDistributionReadThread[] _tArray = null;
	private Hashtable<Integer, Long> _hCounts = new Hashtable<Integer, Long>();
	
	public TestTaxiSummation (TestTaxiRidesDistributionReadThread[] tArray) {
		this._tArray = tArray;
	}
	
	public Hashtable<Integer, Long> getSummation() {
		return this._hCounts;
	}

	public void run() {
		
		for (int i = 0; i < this._tArray.length; i++) {
			TestTaxiRidesDistributionReadThread t = this._tArray[i];
			Hashtable<Integer, ImmutableRoaringBitmap> h = t.getDistributions();
			Enumeration<Integer> e = h.keys();
			while(e.hasMoreElements()) {
				int key = e.nextElement();
				Long sum = this._hCounts.get(key);
				if (sum == null)
					sum = 0L;
				sum += h.get(key).getCardinality();
				this._hCounts.put(key, sum);
			}
		}
	}
}


class TestTaxiRidesGrouping implements Runnable {
	
	TestTaxiRidesDistributionReadThread _group1 = null;
	TestTaxiRidesDistributionReadThread _group2 = null;
	HashMap<String, Integer> _tm = new HashMap<String, Integer>();
	HashMap<String, TestTaxiRidesGroupingThread> _tm1 = new HashMap<String, TestTaxiRidesGroupingThread>();
	
	public TestTaxiRidesGrouping(TestTaxiRidesDistributionReadThread group1, TestTaxiRidesDistributionReadThread group2) {
		this._group1 = group1;
		this._group2 = group2;
	}

	public HashMap<String, Integer> getgroupingCounts() {
		return this._tm;
	}
	
	public void run() {
		Hashtable<Integer, ImmutableRoaringBitmap> h1 = this._group1.getDistributions();
		Hashtable<Integer, ImmutableRoaringBitmap> h2 = this._group2.getDistributions();
		
		ThreadGroup tg = new ThreadGroup ("Grouping Thread : " + System.nanoTime());
		
		try {
			// get the intersection of each of the groups
			Enumeration<Integer> e1 = h1.keys();
			while(e1.hasMoreElements()) {
				int key1 = e1.nextElement();
				Enumeration<Integer> e2 = h2.keys();
				while(e2.hasMoreElements()) {
					int key2 = e2.nextElement();
				
					TestTaxiRidesGroupingThread trg = new TestTaxiRidesGroupingThread(h1.get(key1), h2.get(key2));
					this._tm1.put(key1 + "|" + key2, trg);
					Thread t = new Thread(tg, trg);
					t.start();
				}
			}
		
		
			while (tg.activeCount() > 0)
				Thread.sleep(10);
		
			Set<String> s = this._tm1.keySet();
			Iterator<String> i = s.iterator();
			while(i.hasNext()) {
				String key = i.next();
				this._tm.put(key, this._tm1.get(key).getCount());
				System.out.println(tg.getName() + " : " + key);
			}
		}
		catch(Exception e) {
			System.out.println(tg.getName() + ", Exception");
			e.printStackTrace();
		}
	}
	
}

class TestTaxiRidesGroupingThread implements Runnable {
	
	ImmutableRoaringBitmap _b1 = null;
	ImmutableRoaringBitmap _b2 = null;
	int _count = 0;
	
	public TestTaxiRidesGroupingThread(ImmutableRoaringBitmap b1, ImmutableRoaringBitmap b2) {
		this._b1 = b1;
		this._b2 = b2;
	}

	public int getCount() {
		return this._count;
	}
	
	public void run() {
		// get the intersection of each of the groups
		_count = ImmutableRoaringBitmap.and(_b1, _b2).getCardinality();
	}
	
}