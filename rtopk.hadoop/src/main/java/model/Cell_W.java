package model;

import java.util.Arrays;

public class Cell_W {

	private int id;
	private float[] lowerBound;
	private float[] upperBound;
	
	/**
	 * @param dimensionsNumber the number of dimensions
	 */
	public Cell_W(int id,int dimensionsNumber) {
		super();
		this.id = id;
		lowerBound = new float[dimensionsNumber];
		upperBound = new float[dimensionsNumber];
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the lowerBound
	 */
	public float[] getLowerBound() {
		return lowerBound;
	}

	/**
	 * @param lowerBound the lowerBound to set
	 */
	public void setLowerBound(float[] lowerBound) {
		this.lowerBound = lowerBound;
	}

	/**
	 * @return the upperBound
	 */
	public float[] getUpperBound() {
		return upperBound;
	}

	/**
	 * @param upperBound the upperBound to set
	 */
	public void setUpperBound(float[] upperBound) {
		this.upperBound = upperBound;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Cell_W [lowerBound=" + Arrays.toString(lowerBound)
				+ ", upperBound=" + Arrays.toString(upperBound) + "]";
	}

	
	
	
}
