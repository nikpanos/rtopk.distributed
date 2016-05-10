package grids.gridsS;

import model.Cell_S;
import model.MyItem;

public abstract class GridS {

	abstract public void add(Cell_S cell);
	abstract public int[] getCount(MyItem w, float[] query);
	abstract public int getAntidominateAreaCount(float[] query);
}
