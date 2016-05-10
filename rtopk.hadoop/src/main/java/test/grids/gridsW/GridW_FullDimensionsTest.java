package test.grids.gridsW;

import static org.junit.Assert.assertTrue;
import grids.gridsW.GridW;
import grids.gridsW.GridW_FullDimensions;
import model.Cell_W;
import model.MyItem;

import org.junit.Test;

public class GridW_FullDimensionsTest {
	
	@Test
	public void getRelativeReducerNumberTest(){
		GridW grid = createGrid();
		
		MyItem w1 = new MyItem(1, new float[]{0.0f,1.0f});		
		assertTrue(grid.getRelativeReducerNumber(w1)==0);
		
		MyItem w2 = new MyItem(2, new float[]{1.0f,0.0f});		
		assertTrue(grid.getRelativeReducerNumber(w2)==3);
		
		MyItem w3 = new MyItem(3, new float[]{0.5f,0.5f});
		// TODO look at it carefully!!!
		assertTrue(grid.getRelativeReducerNumber(w3)==1);
	}
	
	/**
	 * <h1>Create the grid for the test</h1>
	 * 
	 * <table>
	 * <tbody>
	 * <tr>
	 * <th>LB_dim1</th>
	 * <th>LB_dim2</th>
	 * <th>UB_dim1</th>
	 * <th>UB_dim2</th>
	 * </tr>
	 * <tr>
	 * <th>0.0</th>
	 * <th>0.75</th>
	 * <th>0.25</th>
	 * <th>1.0</th>
	 * </tr>
	 * <tr>
	 * <th>0.25</th>
	 * <th>0.5</th>
	 * <th>0.5</th>
	 * <th>0.75</th>
	 * </tr>
	 * </tr>
	 * <tr>
	 * <th>0.5</th>
	 * <th>0.25</th>
	 * <th>0.75</th>
	 * <th>0.5</th>
	 * </tr>
	 * <tr>
	 * <th>0.75</th>
	 * <th>0.0</th>
	 * <th>1.0</th>
	 * <th>0.25</th>
	 * </tr>
	 * </tbody>
	 * </table
	 * 
	 * @return the grid
	 */
	private GridW_FullDimensions createGrid(){
		Cell_W cell1 = new Cell_W(0,2);
		cell1.setLowerBound(new float[]{0.0f,0.75f});
		cell1.setUpperBound(new float[]{0.25f,1.0f});
		Cell_W cell2 = new Cell_W(1,2);
		cell2.setLowerBound(new float[]{0.25f,0.5f});
		cell2.setUpperBound(new float[]{0.5f,0.75f});
		Cell_W cell3 = new Cell_W(2,2);
		cell3.setLowerBound(new float[]{0.5f,0.25f});
		cell3.setUpperBound(new float[]{0.75f,0.5f});
		Cell_W cell4 = new Cell_W(3,2);
		cell4.setLowerBound(new float[]{0.75f,0.0f});
		cell4.setUpperBound(new float[]{1.0f,0.25f});
		
		GridW_FullDimensions grid = new GridW_FullDimensions();
		grid.add(cell1);
		grid.add(cell2);
		grid.add(cell3);
		grid.add(cell4);
		
		return grid;
	}
	
	
}
