package grids.gridsW;

import java.util.ArrayList;

import model.Cell_W;
import model.MyItem;

public abstract class GridW {

	abstract public void add(Cell_W segment);
	
	abstract public int getRelativeReducerNumber(MyItem w);
	
	abstract public ArrayList<Cell_W> getSegments();
}
