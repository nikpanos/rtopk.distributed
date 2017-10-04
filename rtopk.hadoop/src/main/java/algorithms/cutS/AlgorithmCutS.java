package algorithms.cutS;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import grids.gridsW.GridW;
import model.Cell_W;
import model.ItemType;
import model.MyItem;
import model.MyKey;

public abstract class AlgorithmCutS {
	
	private static CellComparator comparator = new CellComparator();
	
	public static AlgorithmCutS getAlgorithmCutS(String algorithmName, int gridSegmentation, URI gridWPath, float[] query, int k, Mapper<Object, Text, MyKey, MyItem>.Context context) throws IOException {
		AlgorithmCutS algorithmCutS;
		switch (algorithmName) {
		case "RealBounds":
			algorithmCutS = new AlgorithmS_RealBounds(gridSegmentation, query, context);
			break;
		case "Rlists":
			algorithmCutS = new AlgorithmS_Rlists(k,gridWPath,context);
			break;
		case "NotRealBounds":
			algorithmCutS = new AlgorithmsS_NotRealBounds(gridWPath, query, context);
			break;
		case "CompineNotRealBoundsAndRLists":
			algorithmCutS = new AlgorithmS_CombineNotRealBoundsAndRLists(gridWPath, query, context, k);
			break;
		default:
			throw new IllegalArgumentException("Algorithm for S is not correct!!!");
		}
		
		return algorithmCutS;
	}

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
	abstract public void sendToReducer(MyItem s, ItemType type, int k) throws IOException, InterruptedException;
	
	/**
	 * <h1>Set the key of the current Reducer</h1>
	 * In order to be known to the algorithm in which segment of
	 * grid W the Reducer belongs.
	 * @param key the Reducer key value
	 */
	//abstract public void setReducerKey(int key);
	
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
	abstract public boolean isInLocalAntidominateArea(MyItem s, Cell_W reducerCell);
	
	public int getReducerNumber(MyItem w, double segments) {
		int cellDescriptor;
		double step = 1.0 / segments;
		
		int tupleCellId = 0;
		float[] fields = w.getFields();
		for (int i = 0; i < fields.length; i++) {
			cellDescriptor = (int) (fields[i] / step);
			if (cellDescriptor == segments) {
				cellDescriptor--;
			}
			tupleCellId += cellDescriptor * Math.pow(segments, i);
		}
		
		int result = Collections.binarySearch(grid.getSegments(), new Cell_W(0, 0, tupleCellId), comparator);
		
		try {
		return grid.getSegments().get(result).getId();
		}
		catch (ArrayIndexOutOfBoundsException ex) {
			throw new ArrayIndexOutOfBoundsException(ex.getMessage() + " wItem: " + w.valuesToText().toString());
		}
	}
	
	private static class CellComparator implements Comparator<Cell_W> {
		@Override
		public int compare(Cell_W item1, Cell_W item2) {			
			return item1.getTupleId() - item2.getTupleId();
		}
	}
	
}
