package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import algorithms.Functions;

public class MyListItem {

	private Cell_W segment;
	private Comparator<MyItem> comparator;
	private ArrayList<MyItem> kList;
	private int k;

	/**
	 * @param segment the segment of the grid W
	 * @param k the k number of RTopK
	 */
	public MyListItem(Cell_W segment, int k) {
		super();
		comparator = new K_List_Comparator(segment);
		this.segment = segment;
		this.k = k;
		kList = new ArrayList<MyItem>(k + 1);
	}

	
	
	/**
	 * @return the segment
	 */
	public Cell_W getSegment() {
		return segment;
	}



	/**
	 * @param segment the segment to set
	 */
	public void setSegment(Cell_W segment) {
		this.segment = segment;
	}



	/**
	 * <h1>Add an item S to the K-List</h1>
	 * If the K-List has less than k elements,
	 * then the item added to the K-List.</br>
	 * If the K-List has k elements then there are two cases:</br>
	 * If the cellUpperBound of item S has better score than the 
	 * cellLowerBound of K-List's last item then the item added 
	 * to the K-List and the last item of the K-List removed.</br>
	 * Else the item S doesn't added to the K-List.
	 * 
	 * @param s an item of dataset S
	 * @return true if item s added to the K-List else return false
	 */
	public boolean add(MyItem s) {
		
		if (kList.size() >= k) {
			if (Functions.calculateScore(segment.getUpperBound(),
					kList.get(kList.size() - 1)) < Functions.calculateScore(
					segment.getLowerBound(), s))
				return false;
			else {
				if(Functions.calculateScore(segment.getUpperBound(),
					kList.get(kList.size() - 1)) > Functions.calculateScore(
					segment.getUpperBound(), s)) {
					
					kList.add(s);
					Collections.sort(kList, comparator);
					kList.remove(kList.size()-1);
				}
				return true;
			}
		} 
		else {
			kList.add(s);
			Collections.sort(kList, comparator);
			return true;
		}
		
	}

	/**
	 * <h1>Comparator to sort elements in K-List</h1>
	 * @author George
	 */
	private class K_List_Comparator implements Comparator<MyItem> {

		private Cell_W segment;

		/**
		 * @param segment the segment of the grid W
		 */
		public K_List_Comparator(Cell_W segment) {
			super();
			this.segment = segment;
		}

		@Override
		public int compare(MyItem item1, MyItem item2) {			
			double diff = Functions.calculateScore(segment.getUpperBound(),
					item1)
					- Functions.calculateScore(segment.getUpperBound(), item2);
			if (diff > 0)
				return -1;
			else if (diff < 0)
				return 1;
			else
				return 0;
		}

	}

}
