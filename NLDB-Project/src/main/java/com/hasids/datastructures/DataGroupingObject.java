package com.hasids.datastructures;

import java.util.Enumeration;
import java.util.Hashtable;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

public class DataGroupingObject {

	private int _currentCapacity;
	private MutableRoaringBitmap[] _values = null;
	
	public DataGroupingObject(int initialCapacity) {
		// TODO Auto-generated constructor stub
		this._currentCapacity = initialCapacity;
		_values = new MutableRoaringBitmap[initialCapacity + 1];
		for (int i = 0; i < _values.length; i++)
			_values[i] = new MutableRoaringBitmap();
	}
	
	public void add(int key, int value) {
		if (key >= this._currentCapacity) {
			MutableRoaringBitmap[] valuesTemp = new MutableRoaringBitmap[key + 1];
			for (int i = 0; i < _values.length; i++) {
				valuesTemp[i] = _values[i];
			}
			for (int i = _values.length; i <= key; i++)
				valuesTemp[i] = new MutableRoaringBitmap();
			this._currentCapacity = key;
			this._values = valuesTemp;
			valuesTemp = null;
		}
		
		this._values[key].add(value);
		
	}
	
	public int getEntryCount() {
		return this._currentCapacity;
	}
	
	public MutableRoaringBitmap[] getValues() {
		return this._values;
	}
	
	public static Hashtable<Integer, ImmutableRoaringBitmap> getCumulativeGroupings(DataGroupingObject[] inputArray) {
		Hashtable<Integer, MutableRoaringBitmap> h = new Hashtable<Integer, MutableRoaringBitmap> ();
		
		// iterate through the array
		for(int i = 0; i < inputArray.length; i++) {
			MutableRoaringBitmap[] values = inputArray[i].getValues();
			for (int j = 0; j < values.length; j++) {
				MutableRoaringBitmap temp = h.get(j);
				if (temp == null) {
					h.put(j, values[j]);
				}
				else
					temp.or(values[j]);
			}
		}
		
		Hashtable<Integer, ImmutableRoaringBitmap> hRet = new Hashtable<Integer, ImmutableRoaringBitmap> ();
		Enumeration<Integer> e = h.keys();
		while(e.hasMoreElements()) {
			int key = e.nextElement();
			hRet.put(key, (ImmutableRoaringBitmap) h.remove(key));		
		}
		
		return hRet;
	}

}
