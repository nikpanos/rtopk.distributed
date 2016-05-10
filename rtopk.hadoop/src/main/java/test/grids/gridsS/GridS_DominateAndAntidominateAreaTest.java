package test.grids.gridsS;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import grids.gridsS.GridS;
import grids.gridsS.GridS_DominateAndAntidominateArea;
import model.Cell_S;
import model.MyItem;

import org.junit.Test;

public class GridS_DominateAndAntidominateAreaTest {
	
	@Test
	public void trimToSizeTest(){
		float[] query = new float[]{1.5f,1.5f};
		GridS_DominateAndAntidominateArea grid = createGrid(query);
		
		assertTrue(grid.getAntidominateAreaCount(query) == 5);
		assertTrue(grid.getCells().size() == 7);
		
		for(Cell_S cell : grid.getCells()){
			assertTrue(cell.getId() != 7);
		}
		
		query = new float[]{0.7f,1.5f};
		grid = createGrid(query);

		assertTrue(grid.getAntidominateAreaCount(query) == 0);
		assertTrue(grid.getCells().size() == 7);
		
		for(Cell_S cell : grid.getCells()){
			assertTrue(cell.getId() != 6);
			assertTrue(cell.getId() != 7);
		}
		
		query = new float[]{1.0f,1.0f};
		grid = createGrid(query);
		
		assertTrue(grid.getAntidominateAreaCount(query) == 5);
		assertTrue(grid.getCells().size() == 4);
		
		for(Cell_S cell : grid.getCells()){
			assertTrue(cell.getId() != 4);
			assertTrue(cell.getId() != 5);
			assertTrue(cell.getId() != 6);
			assertTrue(cell.getId() != 7);
		}
		
		query = new float[]{0.0f,0.0f};
		grid = createGrid(query);
		
		assertTrue(grid.getAntidominateAreaCount(query) == 0);
		assertTrue(grid.getCells().size() == 0);
		
		query = new float[]{3.0f,3.0f};
		grid = createGrid(query);
		
		assertTrue(grid.getAntidominateAreaCount(query) == 52);
		assertTrue(grid.getCells().size() == 0);
	}

	@Test
	public void getCountTest(){
		float[] query = new float[]{1.5f,1.5f};
		GridS grid = createGrid(query);

		MyItem w1 = new MyItem(1, new float[]{0.0f,1.0f});
		int[] responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==27 && responce1[1]==46);
		
		query = new float[]{1.2f,1.2f};
		grid = createGrid(query);

		w1 = new MyItem(1, new float[]{0.5f,0.5f});
		responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==5 && responce1[1]==44);
		
		query = new float[]{0.5f,2.5f};
		grid = createGrid(query);

		w1 = new MyItem(1, new float[]{1f,0.0f});
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
	 * @param query the query
	 * @return the grid
	 */
	private GridS_DominateAndAntidominateArea createGrid(float[] query){
		Cell_S cell1 = new Cell_S(1, 5, 2);
		cell1.setLowerBound(new float[]{0.0f,0.0f});
		cell1.setUpperBound(new float[]{1.0f,1.0f});
		Cell_S cell2 = new Cell_S(2, 10, 2);
		cell2.setLowerBound(new float[]{1.0f,0.0f});
		cell2.setUpperBound(new float[]{2.0f,1.0f});
		Cell_S cell3 = new Cell_S(3, 8, 2);
		cell3.setLowerBound(new float[]{0.0f,1.0f});
		cell3.setUpperBound(new float[]{1.0f,2.0f});
		Cell_S cell4 = new Cell_S(4, 7, 2);
		cell4.setLowerBound(new float[]{1.0f,1.0f});
		cell4.setUpperBound(new float[]{2.0f,2.0f});
		Cell_S cell5 = new Cell_S(5, 4, 2);
		cell5.setLowerBound(new float[]{2.0f,1.0f});
		cell5.setUpperBound(new float[]{3.0f,2.0f});
		Cell_S cell6 = new Cell_S(6, 3, 2);
		cell6.setLowerBound(new float[]{1.0f,2.0f});
		cell6.setUpperBound(new float[]{2.0f,3.0f});
		Cell_S cell7 = new Cell_S(7, 1, 2);
		cell7.setLowerBound(new float[]{2.0f,2.0f});
		cell7.setUpperBound(new float[]{3.0f,3.0f});
		Cell_S cell8 = new Cell_S(8, 2, 2);
		cell8.setLowerBound(new float[]{0.0f,2.0f});
		cell8.setUpperBound(new float[]{1.0f,3.0f});
		Cell_S cell9 = new Cell_S(9, 12, 2);
		cell9.setLowerBound(new float[]{2.0f,0.0f});
		cell9.setUpperBound(new float[]{3.0f,1.0f});
		
		ArrayList<Cell_S> cells = new ArrayList<Cell_S>();
		
		cells.add(cell1);
		cells.add(cell2);
		cells.add(cell3);
		cells.add(cell4);
		cells.add(cell5);
		cells.add(cell6);
		cells.add(cell7);
		cells.add(cell8);
		cells.add(cell9);
		
		return new GridS_DominateAndAntidominateArea(cells,query);
	}
}
