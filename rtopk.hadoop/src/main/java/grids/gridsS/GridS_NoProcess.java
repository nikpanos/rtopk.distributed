package grids.gridsS;

import model.Cell_S;
import model.MyItem;

public class GridS_NoProcess extends GridS {

	@Override
	public void add(Cell_S cell) {
	}

	@Override
	public int[] getCount(MyItem w, float[] query) {
		return new int[] {0, Integer.MAX_VALUE};
	}

	@Override
	public int getAntidominateAreaCount(float[] query) {
		return 0;
	}

}
