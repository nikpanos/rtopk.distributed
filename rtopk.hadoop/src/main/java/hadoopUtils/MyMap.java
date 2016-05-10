package hadoopUtils;

import grids.gridsS.GridS;
import grids.gridsS.GridS_DominateAndAntidominateArea;
import grids.gridsS.GridS_Simple;
import grids.gridsS.GridS_Tree;
import grids.gridsS.GridS_TreeDominateAndAntidominateArea;
import hadoopUtils.counters.MyCounters;

import java.io.IOException;

import model.DocumentLine;
import model.ItemType;
import model.MyItem;
import model.MyKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import algorithms.Dominance;
import algorithms.FileParser;
import algorithms.cutS.AlgorithmCutS;
import algorithms.cutS.AlgorithmS_CombineNotRealBoundsAndRLists;
import algorithms.cutS.AlgorithmS_RealBounds;
import algorithms.cutS.AlgorithmS_Rlists;
import algorithms.cutS.AlgorithmsS_NotRealBounds;

public class MyMap extends
		Mapper<LongWritable, DocumentLine, MyKey, MyItem> {

	// The query
	private static float[] q;
	// The K of RTOPk
	private static int k;

	// The name of the file that contains the dataset S
	private static String fileName_S;
	// The name of the file that contains the dataset W
	private static String fileName_W;

	// The grid of dataset S
	private static GridS gridS;
	
	// The grid of dataset W
	private static AlgorithmCutS algorithmCutS;
		
	private static boolean antidominateAreaElementsMoreThanK;
	
	// setup executed once at the beginning of the Mapper
	// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Mapper.html
	@Override
	protected void setup(
			Mapper<LongWritable, DocumentLine, MyKey, MyItem>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		// initialize �
		k = context.getConfiguration().getInt("K", 0);
		if (k <= 0)
			throw new IllegalArgumentException("K is not set!!!");

		// initialize the filename of the file that contains dataset S
		fileName_S = context.getConfiguration().get("FileName_S");

		if (fileName_S == null || fileName_S.trim().equals(""))
			throw new IllegalArgumentException("FileName S is not set!!!");

		// initialize the filename of the file that contains dataset W
		fileName_W = context.getConfiguration().get("FileName_W");

		if (fileName_W == null || fileName_W.trim().equals(""))
			throw new IllegalArgumentException("FileName W is not set!!!");

		// Initialize the dimensions number of the query
		int queryDimentions = context.getConfiguration().getInt(
				"queryDimentions", 0);

		if (queryDimentions < 1)
			throw new IllegalArgumentException("Query Dimentions is not set!!!");

		// Initialize the array that contains the value of each dimension of the query
		q = new float[queryDimentions];

		// add values to the array
		for (int i = 0; i < queryDimentions; i++) {
			float value = context.getConfiguration().getFloat("queryDim" + i,
					-1);
			if (value < 0)
				throw new IllegalArgumentException("Dimention " + i
						+ " is not set!!!");
			q[i] = value;
		}
				
		if(context.getCacheFiles().length!=2)
			throw new IllegalArgumentException("Files that contains the Grid for S and W is not set!!!");
		
		Path gridSPath = context.getLocalCacheFiles()[0];
		Path gridWPath = context.getLocalCacheFiles()[1];
		
		context.setStatus("Create GridS");
		System.out.println(context.getStatus());
		
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
		System.out.println(context.getStatus());
		context.setStatus("Create GridW");
		System.out.println(context.getStatus());
		
		switch (context.getConfiguration().get("AlgorithmForS")) {
		case "RealBounds":
			algorithmCutS = new AlgorithmS_RealBounds(gridWPath, q, context);
			break;
		case "Rlists":
			algorithmCutS = new AlgorithmS_Rlists(k,gridWPath,context);
			break;
		case "NotRealBounds":
			algorithmCutS = new AlgorithmsS_NotRealBounds(gridWPath, q, context);
			break;
		case "CompineNotRealBoundsAndRLists":
			algorithmCutS = new AlgorithmS_CombineNotRealBoundsAndRLists(gridWPath, q, context,k);
			break;
		default:
			throw new IllegalArgumentException("Algorithm for S is not correct!!!");
		};
		

		context.setStatus("GridW Created!!!");
		System.out.println(context.getStatus());
				
		int antidominateAreaElementsCount = gridS.getAntidominateAreaCount(q);
		if (antidominateAreaElementsCount > 0) {
			context.getCounter(MyCounters.S_Elements_In_Antidominance_Area_Of_GridS).setValue(antidominateAreaElementsCount);
		}
		if(antidominateAreaElementsCount>=k)
			antidominateAreaElementsMoreThanK = true;
		
	}

	public void map(LongWritable key, DocumentLine value, Context context)
			throws IOException, InterruptedException {

		MyItem item = FileParser.parseDatasetElement(value.getText().toString());

		// If the current element belongs to dataset S, then...
		if (fileName_S.equals(value.getFile().toString())) {
			
			long startTime = System.nanoTime();
			
			item.setItemType(ItemType.S);
			context.getCounter(MyCounters.S).increment(1);			
			
			// If the current item dominate or is in tie with q, then...
			if (Dominance.dominate(item.values, q) >= 0) {
				context.getCounter(MyCounters.S1).increment(1);
				
				algorithmCutS.sendToReducer(item);
				
			}
			else {
				context.getCounter(MyCounters.S_pruned_by_domination).increment(1);
			}
			
			long estimatedTime = (System.nanoTime() - startTime) / 1000000;
			
			context.getCounter(MyCounters.Total_effort_for_pruning_S_in_MilliSeconds).increment(estimatedTime);

		}
		// Else if the current element belongs to dataset W, then...
		else if (fileName_W.equals(value.getFile().toString())) {
			
			long startTime = System.nanoTime();
			
			item.setItemType(ItemType.W);
			context.getCounter(MyCounters.W).increment(1);
			
			int reducerNumber = algorithmCutS.getGridW().getRelativeReducerNumber(item);
			
			int[] range = gridS.getCount(item, q);
			
			if(k<range[0])
				context.getCounter(MyCounters.W_pruned_by_GridS).increment(1);
			else if(range[1]<k) {
				item.setItemType(ItemType.W_InTopK);
				context.write(new MyKey(new IntWritable(reducerNumber),item.getItemType()), item);
				context.getCounter(MyCounters.W_in_RTOPk).increment(1);
			}
			else {
				context.write(new MyKey(new IntWritable(reducerNumber),item.getItemType()), item);
				context.getCounter(MyCounters.W1).increment(1);
			}
			
			long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
			
			context.getCounter(MyCounters.Total_effort_for_pruning_W_in_MilliSeconds).increment(estimatedTime);

			
		}
		// Else if the current element is not S or W, then...
		else {
			// Throw exception that the filenames are wrong
			throw new IllegalArgumentException("Wrong Filenames!!!");
		}
				

	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			if (!antidominateAreaElementsMoreThanK) {
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
				}
			}
		} finally {
			cleanup(context);
		}
	}
	
	// Cleanup executed once at the end of the Mapper
	// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Mapper.html
	@Override
	protected void cleanup(
			Mapper<LongWritable, DocumentLine, MyKey, MyItem>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

}
