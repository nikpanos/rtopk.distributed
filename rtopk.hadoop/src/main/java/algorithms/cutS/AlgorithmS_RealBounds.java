package algorithms.cutS;

import hadoopUtils.counters.MyCounters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import model.ItemType;
import model.MyItem;
import model.MyKey;
import trueGrid.AngleCell;
import trueGrid.AngleGrid;

public class AlgorithmS_RealBounds extends AlgorithmCutS {
	
	/**
	 * The Mapper's context
	 */
	private Mapper<Object, Text, MyKey, MyItem>.Context contextMapper;
	/**
	 * The query
	 */
	//private float[] query;
	/**
	 * The current reducer cell
	 */
	private AngleCell reducerCell;
	
	private AngleGrid grid;
	
	private String[] readAllLines(URI path) throws IOException {
		ArrayList<String> result = new ArrayList<>();
		File gridFile = new File(path.toString());
		BufferedReader in = new BufferedReader(new FileReader(gridFile));
		try {
			String line;
			while((line = in.readLine()) != null) {
				result.add(line);
			}
			return result.toArray(new String[0]);
		}
		finally {
			in.close();
		}
	}
		
	public AlgorithmS_RealBounds(URI gridWPath, float[] query, Mapper<Object, Text, MyKey, MyItem>.Context contextMapper) throws IOException {
		super();
		this.contextMapper = contextMapper;
		//this.query = query;
		
		//long startTime = System.nanoTime();
		String[] fileLines = readAllLines(gridWPath);
		grid = new AngleGrid(fileLines, query);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextMapper.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
	}
	
	public AlgorithmS_RealBounds(URI gridWPath, float[] query) throws IOException {
		super();
		//this.query = query;
		
		//long startTime = System.nanoTime();

		String[] fileLines = readAllLines(gridWPath);
		grid = new AngleGrid(fileLines, query);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextReducer.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
	}
	
	@Override
	public int getReducerNumber(MyItem w, double segments) {
		return grid.getCellIdByCords(w.values);
	}
	
	@Override
	public void sendToReducer(MyItem s, ItemType type) throws IOException, InterruptedException {
		// if is under UpperBound or under LowerBound send to Reducer (Not in dominate area)
		AngleCell cell;
		for (int i = 0; i < grid.getCells().length; i++) {
			cell = grid.getCells()[i];
			if (cell.qIsBetterRankedThanP(s.values)) {
				contextMapper.getCounter(MyCounters.S2_pruned_by_GridW).increment(1);
			}
			else {
				contextMapper.getCounter(MyCounters.S2_by_mapper).increment(1);
				contextMapper.write(new MyKey(i, type), s);
			}
		}	
		
	}

	@Override
	public void setReducerKey(int key) {
		reducerCell = grid.getCells()[key];
	}

	@Override
	public boolean isInLocalAntidominateArea(MyItem s) {
		// if is under both LowerBound and UpperBound, then is in antidominate area.
		return reducerCell.pIsBetterRankedThanQ(s.values);
	}
	
	

}
