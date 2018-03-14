/**
 * 
 * @author Durga Turaga
 * @since 08/20/2017
 * @copyright A4DATA LLC; All rights reserved
 *
 */

package com.hasids.datastructures;

public class HASIDSFactStructure {

	private float[] _f;
	private double[] _d;
	
	public static int TYPE_NONE = 0;
	public static int TYPE_FLOAT = 1;
	public static int TYPE_DOUBLE = 2;
	public static int TYPE_BOTH = 3;
	
	private int _type = HASIDSFactStructure.TYPE_NONE;
	
	public HASIDSFactStructure() {
		// TODO Auto-generated constructor stub
	}
	
	public void setFloatValues(float[] f) {
		if (this._type == HASIDSFactStructure.TYPE_NONE)
			this._type = HASIDSFactStructure.TYPE_FLOAT;
		
		if (this._type == HASIDSFactStructure.TYPE_DOUBLE)
			this._type = HASIDSFactStructure.TYPE_BOTH;
		
		this._f = f;
	}

	public void setDoubleValues(double[] d) {
		if (this._type == HASIDSFactStructure.TYPE_NONE)
			this._type = HASIDSFactStructure.TYPE_DOUBLE;
		
		if (this._type == HASIDSFactStructure.TYPE_FLOAT)
			this._type = HASIDSFactStructure.TYPE_BOTH;
		
		this._d = d;
	}
	
	public int getType() {
		return this._type;
	}
	
	public float[] getFloatValues() throws Exception {
		if (this._f == null)
			throw new Exception ("Float values not set!");
		
		return this._f;
	}
	
	public double[] getDoubleValues() throws Exception {
		if (this._d == null)
			throw new Exception ("Float values not set!");
		
		return this._d;
	}
}
