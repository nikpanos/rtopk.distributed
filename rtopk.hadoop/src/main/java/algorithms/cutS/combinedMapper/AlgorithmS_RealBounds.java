package algorithms.cutS.combinedMapper;

import grids.gridsW.GridW_NotFullDimensions;
import hadoopUtils.counters.MyCounters;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import algorithms.Functions;
import model.Cell_W;
import model.DocumentLine;
import model.ItemType;
import model.MyItem;
import model.MyKey;

public class AlgorithmS_RealBounds extends AlgorithmCutS {
	
	/**
	 * The Mapper's context
	 */
	private Mapper<LongWritable, DocumentLine, MyKey, MyItem>.Context contextMapper;
	/**
	 * The query
	 */
	private float[] query;
	/**
	 * The current reducer cell
	 */
	private Cell_W reducerCell;
		
	public AlgorithmS_RealBounds(URI gridWPath, float[] query, Mapper<LongWritable, DocumentLine, MyKey, MyItem>.Context contextMapper) throws IOException {
		super();
		this.contextMapper = contextMapper;
		this.query = query;
		
		//long startTime = System.nanoTime();

		grid = new GridW_NotFullDimensions(gridWPath);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextMapper.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
	}
	
	public AlgorithmS_RealBounds(URI gridWPath, float[] query, Reducer<MyKey, MyItem, Text, Text>.Context contextReducer) throws IOException {
		super();
		this.query = query;
		
		//long startTime = System.nanoTime();

		grid = new GridW_NotFullDimensions(gridWPath);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextReducer.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
	}
	
	@Override
	public void sendToReducer(MyItem s, ItemType type) throws IOException, InterruptedException {
		// if is under UpperBound or under LowerBound send to Reducer (Not in dominate area) 
		for (int i=0;i<grid.getSegments().size();i++) {
			if(Functions.calculateScore(grid.getSegments().get(i).getLowerBound(), s) 
					< Functions.calculateScore(grid.getSegments().get(i).getLowerBound(), query)
					||
				Functions.calculateScore(grid.getSegments().get(i).getUpperBound(), s) 
					< Functions.calculateScore(grid.getSegments().get(i).getUpperBound(), query)
					){
				
				contextMapper.getCounter(MyCounters.S2).increment(1);
				contextMapper.write(new MyKey(grid.getSegments().get(i).getId(), type), s);
			}
			else
				contextMapper.getCounter(MyCounters.S2_pruned_by_GridW).increment(1);
		}	
		
	}

	@Override
	public void setReducerKey(int key) {
		for(Cell_W cell : getGridW().getSegments()){
			if(cell.getId()==key) {
				reducerCell = cell;
				break;
			}
		}
	}

	@Override
	public boolean isInLocalAntidominateArea(MyItem s) {
		// if is under both LowerBound and UpperBound, then is in antidominate area.
		if (Functions.calculateScore(reducerCell.getLowerBound(), s) 
				< Functions.calculateScore(reducerCell.getLowerBound(), query)
				&&
			Functions.calculateScore(reducerCell.getUpperBound(), s) 
				< Functions.calculateScore(reducerCell.getUpperBound(), query)
				) {
			return true;
		}
		return false;
	}
	
	

}
