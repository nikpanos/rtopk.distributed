package grids.gridsS;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import algorithms.FileParser;
import algorithms.Functions;
import model.Cell_S;
import model.MyItem;

public class GridS_Simple extends GridS {

	private ArrayList<Cell_S> cells = new ArrayList<Cell_S>();
	
	public GridS_Simple() {
		super();
	}
	
	public GridS_Simple(URI gridSPath) throws IOException {
		super();
		FileParser.parseGridSFile(gridSPath, this);
		cells.trimToSize();
	}

	public void add(Cell_S cell){
		cells.add(cell);
	}
	
	public int[] getCount(MyItem w, float[] query){
		int min = 0;
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
		int count = 0;
		for (Cell_S cell : cells) {
			boolean undominate = true;
			for(int i=0;i<query.length;i++){
				if(cell.getUpperBound()[i]>query[i])
					undominate = false;
			}
			if(undominate)
				count += cell.getCount();
		}
		return count;
	}

}
