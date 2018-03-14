package com.hasids.datastructures;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class DataOperations implements Runnable {

	public static final int INTERSECTION = 1;
	public static final int UNION = 1;
	
	ArrayList<Hashtable<Integer, ImmutableRoaringBitmap>> _groupingArray = null;
	Hashtable<String, MutableRoaringBitmap> _resultTable = new Hashtable<String, MutableRoaringBitmap>();
	private int _operation = 0;
	
	public DataOperations(ArrayList<Hashtable<Integer, ImmutableRoaringBitmap>> groupingArray, int operationType) {
		// TODO Auto-generated constructor stub
		if (groupingArray.size() > 0)
			this._groupingArray = groupingArray;
		
		this._operation = operationType;
	}
	
	private void generateIntersection(int begin) {	
		int next = begin + 1;
		// we will now group between the hashtable at begin point and with what we have in the resultstable
		// when begin == 0; there will be no results
		if (begin == 0) {
			
			Enumeration<Integer> e = this._groupingArray.get(begin).keys();
			while(e.hasMoreElements()) {
				int key = e.nextElement();
				this._resultTable.put(key + "", (MutableRoaringBitmap)this._groupingArray.get(begin).get(key));
			}
			
			// There is only one element in the array and we have reached the end
			if (next >= this._groupingArray.size()) 
				return;
			else
				this.generateIntersection(next);
			
		}
		else {
			// we will already have data in the results table
			Enumeration<String> e1 = this._resultTable.keys();
			while(e1.hasMoreElements()) {
				String key1 = e1.nextElement();
				ImmutableRoaringBitmap irb1 = this._resultTable.remove(key1);
				Enumeration<Integer> e2 = this._groupingArray.get(begin).keys();
				while(e2.hasMoreElements()) {
					int key2 = e2.nextElement();
					ImmutableRoaringBitmap irb2 = this._groupingArray.get(begin).get(key2);
					MutableRoaringBitmap mrb = ImmutableRoaringBitmap.and(irb1, irb2);
					this._resultTable.put(key1 + "|" + key2, mrb);
				}
			}
			
			if (next >= this._groupingArray.size()) // we are already at the end of the chaain
				return;
			else
				this.generateIntersection(next);
			
		}
	}

	private void generateUnion() {	
		ImmutableRoaringBitmap imrb = null;
		MutableRoaringBitmap mrb = null;
		for (int i = 0; i < this._groupingArray.size(); i++) {
			Enumeration<Integer> e2 = this._groupingArray.get(i).keys();
			while(e2.hasMoreElements()) {
				int key = e2.nextElement();
				imrb = this._groupingArray.get(i).get(key);
				mrb = this._resultTable.get(key + "");
				if (mrb != null)
					mrb.or(imrb);
				else
					mrb = (MutableRoaringBitmap) imrb;
				
				this._resultTable.put(key + "", mrb);
			}
		}
	}
	
	public Hashtable<String, ImmutableRoaringBitmap> getGroupingResults() {
		Hashtable<String, ImmutableRoaringBitmap> hRet = new Hashtable<String, ImmutableRoaringBitmap> ();
		Enumeration<String> e = this._resultTable.keys();
		while(e.hasMoreElements()) {
			String key = e.nextElement();
			hRet.put(key, (ImmutableRoaringBitmap) this._resultTable.get(key));
		}
		
		return hRet;
	}

	public void run() {
		// TODO Auto-generated method stub
		if (this._groupingArray == null || this._groupingArray.size() <= 0)
			return;
		
		if (this._operation == DataOperations.INTERSECTION)
			generateIntersection(0);
		else if (this._operation == DataOperations.UNION)
			generateUnion();
	}

}
