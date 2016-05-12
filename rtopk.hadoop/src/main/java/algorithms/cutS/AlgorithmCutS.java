package algorithms.cutS;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;

import grids.gridsW.GridW;
import model.Cell_W;
import model.MyItem;

public abstract class AlgorithmCutS {
	
	private static CellComparator comparator = new CellComparator();

	/**
	 * The grid that the algorithm use
	 */
	protected GridW grid;
	
	/**
	 * <h1>Get the grid W</h1>
	 * @see AlgorithmCutS#grid
	 * @return the grid
	 */
	public GridW getGridW(){
		return grid;
	}
	
	/**
	 * <h1>Send the element s to the Reducers</h1>
	 * 
	 * This method send the element s to the appropriate Reducers. 
	 * The each algorithm decide where element s should be sent to.
	 * 
	 * @param s an element of dataset S
	 * @throws IOException
	 * @throws InterruptedException
	 */
	abstract public void sendToReducer(MyItem s) throws IOException, InterruptedException;
	
	/**
	 * <h1>Set the key of the current Reducer</h1>
	 * In order to be known to the algorithm in which segment of
	 * grid W the Reducer belongs.
	 * @param key the Reducer key value
	 */
	abstract public void setReducerKey(int key);
	
	/**
	 * <h1>Check if element S belongs to local antidominate area</h1>
	 * 
	 * This method check if the element s belongs to local antidominate area of the segment (of
	 * grid W) that the Reducer belongs. 
	 * The each algorithm decide when the element s belongs or not to the antidominate area.
	 * 
	 * @param s an element of dataset S
	 * @return true or false if element s belongs to local antidominate area or not
	 */
	abstract public boolean isInLocalAntidominateArea(MyItem s);
	
	public int getReducerNumber(MyItem w, double segments) {
		int cellDescriptor;
		double step = 1.0 / segments;
		
		int tupleCellId = 0;
		float[] fields = w.getFields();
		for (int i = 0; i < fields.length; i++) {
			cellDescriptor = (int) (fields[i] / step);
			tupleCellId += cellDescriptor * Math.pow(segments, i);
		}
		
		int result = Collections.binarySearch(grid.getSegments(), new Cell_W(0, 0, tupleCellId), comparator);
		
		return grid.getSegments().get(result).getId();
	}
	
	private static class CellComparator implements Comparator<Cell_W> {
		@Override
		public int compare(Cell_W item1, Cell_W item2) {			
			return item1.getTupleId() - item2.getTupleId();
		}
	}
	
}
