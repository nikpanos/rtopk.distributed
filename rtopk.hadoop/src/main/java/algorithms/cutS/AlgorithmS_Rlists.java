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

public class AlgorithmS_Rlists extends AlgorithmCutS{
	
	/**
	 * The R-Lists
	 */
	private MyListItem[] lists;
	
	//private Cell_W reducerCell;
	
	private float[] query;
	
	//private int k;
	
	/**
	 * The Mapper's context 
	 */
	private Mapper<Object, Text, MyKey, MyItem>.Context contextMapper;
		
	/**
	 * <h1>Constructor for Map phase</h1>
	 *  This constructor initialize the grid W that the algorithm use and initialize the RLists.
	 *  
	 * @param k the k number of TopK
	 * @param gridWPath the path of grid W
	 * @param contextMapper @see {@link AlgorithmS_Rlists#contextMapper}
	 * @throws IOException
	 */
	public AlgorithmS_Rlists(int k, URI gridWPath, Mapper<Object, Text, MyKey, MyItem>.Context contextMapper) throws IOException {
		super();
		
		this.contextMapper = contextMapper;

		//long startTime = System.nanoTime();

		grid = new GridW_FullDimensions(gridWPath);
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		
		//contextMapper.getCounter(MyCounters.Total_effort_to_load_GridW_in_seconds).increment(estimatedTime);
		
		lists = new MyListItem[grid.getSegments().size()];
		for (int i = 0; i < lists.length; i++) {
			lists[i] = new MyListItem(grid.getSegments().get(i), k);
		}
	}
	
	public AlgorithmS_Rlists(int k, URI gridWPath, float[] query) throws IOException {
		grid = new GridW_FullDimensions(gridWPath);
		lists = new MyListItem[1];
		//lists[0] = new MyListItem(grid.getSegments().get(partitionId), k);
		this.query = query;
		//this.k = k;
	}
	
	/**
	 * <h1>Constructor for Reduce phase</h1>
	 * This algorithm do nothing at reduce phase.
	 * The constructor do nothing.
	 * @param contextReducer the context of Reducer
	 * @throws IOException
	 */
	public AlgorithmS_Rlists() throws IOException {
		super();
	}
	
	public boolean checkPointUsingList(MyItem s) {
		return lists[0].add(s);
	}
	
	@Override
	public void sendToReducer(MyItem s, ItemType type) throws IOException, InterruptedException {
		
		for (int i = 0; i < lists.length; i++) {
			
			if (lists[i].add(s)) {
				contextMapper.write(new MyKey(lists[i].getSegment().getId(), type), s);
				contextMapper.getCounter(MyCounters.S2_by_mapper).increment(1);
			} else
				contextMapper.getCounter(MyCounters.S2_pruned_by_RLists).increment(1);
		}
		
	}
	
	/**
	 * <h1>Set the key of the current Reducer</h1>
	 * This algorithm has no functionality at reduce phase. This method do nothing.
	 */
	/*@Override
	public void setReducerKey(int key){
		for(Cell_W cell : getGridW().getSegments()){
			if(cell.getId()==key) {
				reducerCell = cell;
				break;
			}
		}
		
		lists[0] = new MyListItem(reducerCell, k);
	}*/
	
	/**
	 * <h1>Check if element S belongs to local antidominate area</h1>
	 * This algorithm has no functionality at reduce phase. This method return always false.
	 */
	@Override
	public boolean isInLocalAntidominateArea(MyItem s, Cell_W reducerCell) {
		if (Functions.calculateScore(reducerCell.getUpperBound(),s) 
				< Functions.calculateScore(reducerCell.getLowerBound(), query)) {
			return true;
		}
		return false;
	}

}
