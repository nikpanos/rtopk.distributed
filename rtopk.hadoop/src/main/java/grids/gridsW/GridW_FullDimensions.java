package grids.gridsW;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import algorithms.FileParser;
import model.Cell_W;
import model.MyItem;

public class GridW_FullDimensions extends GridW{

	private ArrayList<Cell_W> segments = new ArrayList<Cell_W>();
	
	public GridW_FullDimensions() {
		super();
	}
		
	public GridW_FullDimensions(URI path) throws IOException {
		super();
		FileParser.parseGridWFullDimentionFile(path, this);
		segments.trimToSize();
	}

	/**
	 * <h1>Add a segment to the Grid</h1>
	 * 
	 * Appends the segment to the end of the list.
	 * 
	 * @param segment
	 *            the segment to be added
	 */
	public void add(Cell_W segment) {
		segments.add(segment);
	}
	
	
	
	/**
	 * @return the segments
	 */
	public ArrayList<Cell_W> getSegments() {
		return segments;
	}

	/**
	 * <h1>Return the reducer number that the w belongs</h1>
	 * 
	 * The Grid for dataset W has some segments. The number of these segments is equal with the reducers number.</br>
	 * 
	 * Suppose that we have a grid with these segments:</br>
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>x</th>
	 * <th>0</th>
	 * <th>...</th>
	 * <th>0.25</th>
	 * <th>...</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>1</th>
	 * </tr>
	 * <tr>
	 * <th>y</th>
	 * <th>1</th>
	 * <th>...</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>0.25</th>
	 * <th>...</th>
	 * <th>0</th>
	 * </tr>
	 * <tr>
	 * <th></th>
	 * <th>[</th>
	 * <th>0</th>
	 * <th>] (</th>
	 * <th>1</th>
	 * <th>] (</th>
	 * <th>2</th>
	 * <th>] (</th>
	 * <th>3</th>
	 * <th>]</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * the item w={0.2,0.8} belongs to segment 0</br> the item w={0.25,0.75}
	 * belongs to segment 0</br> the item w={0.8,0.2} belongs to segment 3</br>
	 * 
	 * @param w an item of dataset W
	 * @return the number of the reducer that the w belongs
	 */
	public int getRelativeReducerNumber(MyItem w) {
		for (int i = 0; i < segments.size(); i++) {
			if (belognsToSegment(segments.get(i), w))
				return segments.get(i).getId();
		}
		throw new IllegalArgumentException("Grid W isn't correct!!!\n" + "id=" + w.getId() +" - "+w.valuesToText());
	}

	/**
	 * <h1>Answered if an item w belongs to a segment</h1>
	 * 
	 * Suppose that we have a grid with these segments:</br>
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>x</th>
	 * <th>0</th>
	 * <th>...</th>
	 * <th>0.25</th>
	 * <th>...</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>1</th>
	 * </tr>
	 * <tr>
	 * <th>y</th>
	 * <th>1</th>
	 * <th>...</th>
	 * <th>0.75</th>
	 * <th>...</th>
	 * <th>0.5</th>
	 * <th>...</th>
	 * <th>0.25</th>
	 * <th>...</th>
	 * <th>0</th>
	 * </tr>
	 * <tr>
	 * <th></th>
	 * <th>[</th>
	 * <th>0</th>
	 * <th>] (</th>
	 * <th>1</th>
	 * <th>] (</th>
	 * <th>2</th>
	 * <th>] (</th>
	 * <th>3</th>
	 * <th>]</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * the item w={0.2,0.8} belongs to segment 1</br> the item w={0.25,0.75}
	 * belongs to segment 1</br> the item w={0.8,0.2} belongs to segment 4</br>
	 * 
	 * @param segment a segment of the grid
	 * @param w an item of dataset W
	 * @return if w belongs to the segment return true else return false
	 */
	private boolean belognsToSegment(Cell_W segment, MyItem w) {
		for (int i = 0; i < w.values.length; i++) {
			if (w.values[i] < segment.getLowerBound()[i])
				return false;
			else if (segment.getUpperBound()[i] < w.values[i])
				return false;
		}
		return true;
	}
}
