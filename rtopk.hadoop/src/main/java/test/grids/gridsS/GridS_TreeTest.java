package test.grids.gridsS;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import algorithms.FileParser;
import grids.gridsS.GridS;
import grids.gridsS.GridS_Simple;
import grids.gridsS.GridS_Tree;
import model.Cell_S;
import model.MyItem;

public class GridS_TreeTest {

	@Test
	public void test() throws IllegalArgumentException, IOException, URISyntaxException{
		GridS grid = new GridS_Tree(4);
		FileParser.parseGridSFile(new URI("C:\\Users\\George\\Desktop\\Suni4.grid"), grid);
		GridS gridSimple = new GridS_Simple();
		FileParser.parseGridSFile(new URI("C:\\Users\\George\\Desktop\\Suni4.grid"), gridSimple);
		
		float[] query = new float[]{13323434.5f,2344345.0f,13232343.5f,1434333.0f};
		
		BufferedReader in = new BufferedReader(new FileReader(new File("C:\\Users\\George\\Desktop\\W.txt")));
		String line;
		
		assertTrue(grid.getAntidominateAreaCount(query)==gridSimple.getAntidominateAreaCount(query));
		
		// while file has more lines, repeat...
		while((line = in.readLine()) != null)
		{
			MyItem item = FileParser.parseDatasetElement(line);
			int[] r1 = grid.getCount(item, query);
			int[] r2 = gridSimple.getCount(item, query);
			assertTrue(r1[0] == r2[0]);
			assertTrue(r1[1] == r2[1]);
			
		}
		in.close();
	}
	
	@Test
	public void test1() throws IllegalArgumentException, IOException{
		GridS grid =  createGrid();
		float[] query = new float[]{1.5f,1.5f};

		MyItem w1 = new MyItem(1, new float[]{0.0f,1.0f});
		int[] responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==27 && responce1[1]==46);
		
		query = new float[]{1.2f,1.2f};

		w1 = new MyItem(1, new float[]{0.5f,0.5f});
		responce1 = grid.getCount(w1, query);
		
		assertTrue( responce1[0]==5 && responce1[1]==44);
		
		query = new float[]{0.5f,2.5f};

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
	 * @return the grid
	 */
	private GridS_Tree createGrid(){
		GridS_Tree grid = new GridS_Tree(2);
		
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
