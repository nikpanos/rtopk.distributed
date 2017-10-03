package grids.gridsS;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import model.Cell_S;
import model.MyItem;

import algorithms.Dominance;
import algorithms.FileParser;
import algorithms.Functions;

public class GridS_DominateAndAntidominateArea extends GridS{

	private ArrayList<Cell_S> cells = new ArrayList<Cell_S>();
	private int antidominateAreaCount = 0;
	
	public GridS_DominateAndAntidominateArea(ArrayList<Cell_S> cells,float[] query) {
		super();
		this.cells = cells;
		trimToSize(query);
	}
	
	public GridS_DominateAndAntidominateArea(URI gridSPath, float[] query) throws IOException {
		super();
		FileParser.parseGridSFile(gridSPath, this);
		trimToSize(query);
	}
	
	public GridS_DominateAndAntidominateArea(BufferedReader gridReader, float[] query) throws IOException {
		super();
		FileParser.parseGridSFile(gridReader, this);
		trimToSize(query);
	}
	
	public void add(Cell_S cell){
		cells.add(cell);
	}
	
	public int[] getCount(MyItem w, float[] query){
		int min = antidominateAreaCount;
		int max = 0;
		double queryScore = Functions.calculateScore(w, query);
		for (Cell_S cell : cells) {			
			// Cell is under the Wi line
			if(queryScore>Functions.calculateScore(w, cell.getUpperBound()))
				min += cell.getCount();
			// Wi line intersect cell
			else if(queryScore>Functions.calculateScore(w, cell.getLowerBound())){
				max += cell.getCount(); 
			}
		}
		max += min;
		
		return new int[] {min,max};
	}
	
	public int getAntidominateAreaCount(float[] query){
		return antidominateAreaCount;
	}
	
	private void trimToSize(float[] query){
		
		ArrayList<Cell_S> copy = new ArrayList<Cell_S>(cells);
		
		for (Cell_S cell : copy) {
			boolean antidominate = true;
			boolean dominate = true;
			if (Dominance.dominateBoostQuery(query,cell.getUpperBound()) != -1) {
				antidominate = false;
			} 
			if (Dominance.dominateBoostQuery(cell.getLowerBound(), query) != -1) {
				dominate = false;
			}
			if (antidominate) {
				antidominateAreaCount += cell.getCount();
				cells.remove(cell);
			}
			if(dominate){
				cells.remove(cell);
			}
		}
		
		cells.trimToSize();
	}

	/**
	 * @return the cells
	 */
	public ArrayList<Cell_S> getCells() {
		return cells;
	}
	
}