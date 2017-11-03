package grids.gridsS;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;

import model.Cell_S;
import model.MyItem;

public abstract class GridS {

	abstract public void add(Cell_S cell);
	abstract public int[] getCount(MyItem w, float[] query);
	abstract public int getAntidominateAreaCount(float[] query);
	
	public static GridS getGrid(String gridName, URI gridSPath, float[] query, int k) throws IOException {
		GridS gridS;
		switch (gridName) {
			case "Simple":
				gridS = new GridS_Simple(gridSPath);
				break;
			case "DominateAndAntidominateArea":
				gridS = new GridS_DominateAndAntidominateArea(gridSPath, query);			
				break;
			case "Tree":
				gridS = new GridS_Tree(gridSPath, query);			
				break;
			case "TreeDominateAndAntidominateArea":
				gridS = new GridS_TreeDominateAndAntidominateArea(gridSPath, query);			
				break;
			case "RTree":
				gridS = new GridS_RTree(gridSPath, query, k);
				break;
			case "NoTree":
				gridS = new GridS_NoProcess();
				break;
			default:
				throw new IllegalArgumentException("Grid for S is not correct!!!");
		}
		
		return gridS;
	}
	
	public static GridS getGrid(String gridName, BufferedReader gridSPath, float[] query, int k) throws IOException {
		GridS gridS;
		switch (gridName) {
			case "Simple":
				gridS = new GridS_Simple(gridSPath);
				break;
			case "DominateAndAntidominateArea":
				gridS = new GridS_DominateAndAntidominateArea(gridSPath, query);			
				break;
			case "Tree":
				gridS = new GridS_Tree(gridSPath, query);			
				break;
			case "TreeDominateAndAntidominateArea":
				gridS = new GridS_TreeDominateAndAntidominateArea(gridSPath, query);			
				break;
			case "RTree": //NoTree and RTree should be the same in this function (used by MyMapReduceDriver)
				return null;
			case "NoTree":
				gridS = new GridS_RTree(gridSPath, query, k);
				break;
			default:
				throw new IllegalArgumentException("Grid for S is not correct!!!");
		}
		
		return gridS;
	}
}
