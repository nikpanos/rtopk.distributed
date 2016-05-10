package model;

import java.util.Arrays;

public class Cell_S {
	private int id;
	private int count;
	private float[] lowerBound;
	private float[] upperBound;

	public Cell_S(int id, int count, int dimentionsNo) {
		super();
		this.id = id;
		this.count = count;
		lowerBound = new float[dimentionsNo];
		upperBound = new float[dimentionsNo];
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public float[] getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(float[] lowerBound) {
		this.lowerBound = lowerBound;
	}

	public float[] getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(float[] upperBound) {
		this.upperBound = upperBound;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + count;
		result = prime * result + id;
		result = prime * result + Arrays.hashCode(lowerBound);
		result = prime * result + Arrays.hashCode(upperBound);
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cell_S other = (Cell_S) obj;
		if (count != other.count)
			return false;
		if (id != other.id)
			return false;
		if (!Arrays.equals(lowerBound, other.lowerBound))
			return false;
		if (!Arrays.equals(upperBound, other.upperBound))
			return false;
		return true;
	}
	
	
}
