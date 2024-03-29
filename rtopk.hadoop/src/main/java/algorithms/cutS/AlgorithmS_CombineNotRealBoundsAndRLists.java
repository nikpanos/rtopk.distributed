package algorithms.cutS;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import algorithms.Functions;
import grids.gridsW.GridW_FullDimensions;
import hadoopUtils.counters.MyCounters;
import model.Cell_W;
import model.ItemType;
import model.MyItem;
import model.MyKey;
import model.MyListItem;

public class AlgorithmS_CombineNotRealBoundsAndRLists extends AlgorithmCutS {
	
	/**
	 * The R-Lists
	 */
	private MyListItem[] lists;
	
	private Mapper<Object, Text, MyKey, MyItem>.Context contextMapper;
	private float[] query;
	//private Cell_W reducerCell;
		
	public AlgorithmS_CombineNotRealBoundsAndRLists(URI gridWPath, float[] query,
			Mapper<Object, Text, MyKey, MyItem>.Context contextMapper,int k) throws IOException {
		super();
		this.contextMapper = contextMapper;
		this.query = query;
		
		//long startTime = System.nanoTime();

		grid = new GridW_FullDimensions(gridWPath);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextMapper.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
		
		lists = new MyListItem[grid.getSegments().size()];
		for (int i = 0; i < lists.length; i++) {
			lists[i] = new MyListItem(grid.getSegments().get(i), k);
		}
	}
	
	public AlgorithmS_CombineNotRealBoundsAndRLists(URI gridWPath, float[] query) throws IOException {
		super();
		this.query = query;
		
		//long startTime = System.nanoTime();

		grid = new GridW_FullDimensions(gridWPath);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextReducer.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
	}
	
	@Override
	public void sendToReducer(MyItem s, ItemType type, int k) throws IOException, InterruptedException {
		Cell_W segment;
		for (int i = 0; i < grid.getSegments().size(); i++) {
			segment = grid.getSegments().get(i);
			if(Functions.calculateScore(segment.getLowerBound(), s) > Functions.calculateScore(segment.getUpperBound(), query)) {
				contextMapper.getCounter(MyCounters.S2_pruned_by_GridW).increment(1);
			}
			else if (segment.getCountInAntidominance() >= k) {
				contextMapper.getCounter(MyCounters.S2_pruned_by_Cell_Antidominance_Area).increment(1);
			}
			else if (isInLocalAntidominateArea(s, segment)) {
				segment.incCountInAntidominance();
				contextMapper.getCounter(MyCounters.S2_by_mapper_antidominance_area).increment(1);
				contextMapper.write(new MyKey(segment.getId(), ItemType.S_antidom), s);
			}
			else {
				if(lists[i].add(s)){
					contextMapper.getCounter(MyCounters.S2_by_mapper).increment(1);
					contextMapper.write(new MyKey(segment.getId(), type), s);
				}
				else
					contextMapper.getCounter(MyCounters.S2_pruned_by_RLists).increment(1);
			}
				
		}		
	}

	/*@Override
	public void setReducerKey(int key) {
		for(Cell_W cell : getGridW().getSegments()){
			if(cell.getId()==key) {
				reducerCell = cell;
				break;
			}
		}
	}*/

	@Override
	public boolean isInLocalAntidominateArea(MyItem s, Cell_W reducerCell) {
		// if is in antidominate area
		if (Functions.calculateScore(reducerCell.getUpperBound(),s) 
				< Functions.calculateScore(reducerCell.getLowerBound(), query)) {
			return true;
		}
		return false;
	}

}
