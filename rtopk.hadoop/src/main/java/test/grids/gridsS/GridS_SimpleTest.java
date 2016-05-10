package test.grids.gridsS;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import grids.gridsS.GridS;
import grids.gridsS.GridS_Simple;
import model.Cell_S;
import model.MyItem;

public class GridS_SimpleTest {

	@Test
	public void getCountTest(){
		GridS grid = createGrid();
		float[] query = new float[]{(float) 1.5,(float) 1.5};

		MyItem w1 = new MyItem(1, new float[]{(float) 0.0,(float) 1.0});
		int[] responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==27 && responce1[1]==46);
		
		query = new float[]{(float) 1.2,(float) 1.2};

		w1 = new MyItem(1, new float[]{(float) 0.5,(float) 0.5});
		responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==5 && responce1[1]==44);
		
		query = new float[]{(float) 0.5,(float) 2.5};

		w1 = new MyItem(1, new float[]{1,(float) 0.0});
		responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==0 && responce1[1]==15);
	}
	
	/**
	 * <h1>Create the grid for the test</h1>
	 * <table border="1">
	 * <tbody>
	 * <tr>
	 * <th>2</th>
	 * <th>3</th>
	 * <th>1</th>
	 * </tr>
	 * <tr>
	 * <th>8</th>
	 * <th>7</th>
	 * <th>4</th>
	 * </tr>
	 * <tr>
	 * <th>5</th>
	 * <th>10</th>
	 * <th>12</th>
	 * </tr>
	 * </tbody>
	 * </table>
	 * 
	 * @return the grid
	 */
	private GridS_Simple createGrid(){
		GridS_Simple grid = new GridS_Simple();
		
		Cell_S cell1 = new Cell_S(1, 5, 2);
		cell1.setLowerBound(new float[]{(float) 0.0,(float) 0.0});
		cell1.setUpperBound(new float[]{(float) 1.0,(float) 1.0});
		Cell_S cell2 = new Cell_S(2, 10, 2);
		cell2.setLowerBound(new float[]{(float) 1.0,(float) 0.0});
		cell2.setUpperBound(new float[]{(float) 2.0,(float) 1.0});
		Cell_S cell3 = new Cell_S(3, 8, 2);
		cell3.setLowerBound(new float[]{(float) 0.0,(float) 1.0});
		cell3.setUpperBound(new float[]{(float) 1.0,(float) 2.0});
		Cell_S cell4 = new Cell_S(4, 7, 2);
		cell4.setLowerBound(new float[]{(float) 1.0,(float) 1.0});
		cell4.setUpperBound(new float[]{(float) 2.0,(float) 2.0});
		Cell_S cell5 = new Cell_S(5, 4, 2);
		cell5.setLowerBound(new float[]{(float) 2.0,(float) 1.0});
		cell5.setUpperBound(new float[]{(float) 3.0,(float) 2.0});
		Cell_S cell6 = new Cell_S(6, 3, 2);
		cell6.setLowerBound(new float[]{(float) 1.0,(float) 2.0});
		cell6.setUpperBound(new float[]{(float) 2.0,(float) 3.0});
		Cell_S cell7 = new Cell_S(7, 1, 2);
		cell7.setLowerBound(new float[]{(float) 2.0,(float) 2.0});
		cell7.setUpperBound(new float[]{(float) 3.0,(float) 3.0});
		Cell_S cell8 = new Cell_S(8, 2, 2);
		cell8.setLowerBound(new float[]{(float) 0.0,(float) 2.0});
		cell8.setUpperBound(new float[]{(float) 1.0,(float) 3.0});
		Cell_S cell9 = new Cell_S(9, 12, 2);
		cell9.setLowerBound(new float[]{(float) 2.0,(float) 0.0});
		cell9.setUpperBound(new float[]{(float) 3.0,(float) 1.0});
		
		grid.add(cell1);
		grid.add(cell2);
		grid.add(cell3);
		grid.add(cell4);
		grid.add(cell5);
		grid.add(cell6);
		grid.add(cell7);
		grid.add(cell8);
		grid.add(cell9);
		
		return grid;
	}
	
}
