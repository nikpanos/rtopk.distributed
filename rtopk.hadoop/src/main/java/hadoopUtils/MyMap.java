package hadoopUtils;

import grids.gridsS.GridS;
import grids.gridsS.GridS_DominateAndAntidominateArea;
import grids.gridsS.GridS_Simple;
import grids.gridsS.GridS_Tree;
import grids.gridsS.GridS_TreeDominateAndAntidominateArea;
import hadoopUtils.counters.MyCounters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import model.ItemType;
import model.MyItem;
import model.MyKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import algorithms.Dominance;
import algorithms.FileParser;
import algorithms.cutS.AlgorithmCutS;
import algorithms.cutS.AlgorithmS_CombineNotRealBoundsAndRLists;
import algorithms.cutS.AlgorithmS_RealBounds;
import algorithms.cutS.AlgorithmS_Rlists;
import algorithms.cutS.AlgorithmsS_NotRealBounds;

public class MyMap extends Mapper<Object, Text, MyKey, MyItem> {

	// The query
	private static float[] q;
	// The K of RTOPk
	private static int k;

	// The name of the file that contains the dataset S
	private static String fileName_S;
	// The name of the file that contains the dataset W
	//private static String fileName_W;

	// The grid of dataset S
	private static GridS gridS;
	
	// The grid of dataset W
	private static AlgorithmCutS algorithmCutS;
		
	private static boolean antidominateAreaElementsMoreThanK;
	
	private boolean isReadingS;
	
	private int gridWSegmentation;
	
	// setup executed once at the beginning of the Mapper
	// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Mapper.html
	@Override
	protected void setup(Mapper<Object, Text, MyKey, MyItem>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		gridWSegmentation = context.getConfiguration().getInt("gridWSegmentation", 10);
		
		// initialize �
		k = context.getConfiguration().getInt("K", 0);

		// initialize the filename of the file that contains dataset S
		fileName_S = context.getConfiguration().get("FileName_S");

		// initialize the filename of the file that contains dataset W
		//fileName_W = context.getConfiguration().get("FileName_W");
		
		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		isReadingS = filename.equals(fileName_S);

		// Initialize the dimensions number of the query
		int queryDimentions = context.getConfiguration().getInt("queryDimentions", 0);

		// Initialize the array that contains the value of each dimension of the query
		q = new float[queryDimentions];

		// add values to the array
		for (int i = 0; i < queryDimentions; i++) {
			q[i] = context.getConfiguration().getFloat("queryDim" + i, -1);
		}
		
		URI gridSPath = context.getCacheFiles()[0];
		URI gridWPath = context.getCacheFiles()[1];
		
		try {
			gridSPath = new URI(new Path(gridSPath.getPath()).getName());
			gridWPath = new URI(new Path(gridWPath.getPath()).getName());
		} catch (IllegalArgumentException | URISyntaxException e) {
			e.printStackTrace();
		}
		
		if (!isReadingS) {
			context.setStatus("Create GridS");
			//System.out.println(context.getStatus());
			
			// create the grid for dataset S
			//long startTime = System.nanoTime();
			switch (context.getConfiguration().get("GridForS")) {
			case "Simple":
				gridS = new GridS_Simple(gridSPath);
				break;
			case "DominateAndAntidominateArea":
				gridS = new GridS_DominateAndAntidominateArea(gridSPath,q);			
				break;
			case "Tree":
				gridS = new GridS_Tree(gridSPath,q);			
				break;
			case "TreeDominateAndAntidominateArea":
				gridS = new GridS_TreeDominateAndAntidominateArea(gridSPath,q);			
				break;	
			default:
				throw new IllegalArgumentException("Grid for S is not correct!!!");
			}
			//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
			//context.getCounter(MyCounters.Total_effort_to_load_GridS_in_seconds).increment(estimatedTime);
					
			context.setStatus("GridS Created!!!");
			//System.out.println(context.getStatus());
			
			int antidominateAreaElementsCount = gridS.getAntidominateAreaCount(q);
			if (antidominateAreaElementsCount > 0) {
				context.getCounter(MyCounters.S_Elements_In_Antidominance_Area_Of_GridS).setValue(antidominateAreaElementsCount);
			}
			if(antidominateAreaElementsCount>=k)
				antidominateAreaElementsMoreThanK = true;
		
		}
		//else {
			context.setStatus("Create GridW");
			//System.out.println(context.getStatus());
			
			switch (context.getConfiguration().get("AlgorithmForS")) {
			case "RealBounds":
				algorithmCutS = new AlgorithmS_RealBounds(gridWSegmentation, q, context);
				break;
			case "Rlists":
				algorithmCutS = new AlgorithmS_Rlists(k,gridWPath,context);
				break;
			case "NotRealBounds":
				algorithmCutS = new AlgorithmsS_NotRealBounds(gridWPath, q, context);
				break;
			case "CompineNotRealBoundsAndRLists":
				algorithmCutS = new AlgorithmS_CombineNotRealBoundsAndRLists(gridWPath, q, context, k);
				break;
			default:
				throw new IllegalArgumentException("Algorithm for S is not correct!!!");
			}
			
	
			context.setStatus("GridW Created!!!");
			//System.out.println(context.getStatus());
		//}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		MyItem item = FileParser.parseDatasetElement(value.toString());
		// If the current element belongs to dataset S, then...
		if (isReadingS) {
			
			//long startTime = System.nanoTime();
			
			//item.setItemType(ItemType.S);
			context.getCounter(MyCounters.S).increment(1);			
			
			// If the current item dominate or is in tie with q, then...
			if (Dominance.dominate(item.values, q) >= 0) {
				context.getCounter(MyCounters.S1).increment(1);
				
				algorithmCutS.sendToReducer(item, ItemType.S, k);
				
			}
			else {
				context.getCounter(MyCounters.S_pruned_by_domination).increment(1);
			}
			
			//long estimatedTime = (System.nanoTime() - startTime) / 1000000;
			
			//context.getCounter(MyCounters.Total_effort_for_pruning_S_in_MilliSeconds).increment(estimatedTime);

		}
		// Else if the current element belongs to dataset W, then...
		else {
			
			//long startTime = System.nanoTime();
			
			//item.setItemType(ItemType.W);
			context.getCounter(MyCounters.W).increment(1);
			
			int[] range = gridS.getCount(item, q);
			
			if(k < range[0])
				context.getCounter(MyCounters.W_pruned_by_GridS).increment(1);
			else {
				//int reducerNumber = algorithmCutS.getGridW().getRelativeReducerNumber(item);
				int reducerNumber = algorithmCutS.getReducerNumber(item, gridWSegmentation);
				if(range[1] < k) {
					//item.setItemType(ItemType.W_InTopK);
					context.write(new MyKey(reducerNumber, ItemType.W_InTopK), item);
					context.getCounter(MyCounters.W_in_RTOPk).increment(1);
				}
				else {
					context.write(new MyKey(reducerNumber, ItemType.W), item);
					context.getCounter(MyCounters.W1).increment(1);
				}
			}
			
			//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
			
			//context.getCounter(MyCounters.Total_effort_for_pruning_W_in_MilliSeconds).increment(estimatedTime);
		}
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			if (!antidominateAreaElementsMoreThanK) {
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			}
		} finally {
			cleanup(context);
		}
	}
	
	// Cleanup executed once at the end of the Mapper
	// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Mapper.html
	@Override
	protected void cleanup(Mapper<Object, Text, MyKey, MyItem>.Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
