package hadoopUtils;

import hadoopUtils.counters.MyCounters;

import java.io.IOException;
import java.util.ArrayList;

import model.ItemType;
import model.MyItem;
import model.MyKey;
import model.RtopkAlgorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import algorithms.BrsWithNewTree;
import algorithms.Rta;

public class MyReducer extends Reducer<MyKey, MyItem, Text, Text> {

	// The query
	private static float[] q;
	// The K of RTOPk
	private static int k;

	// The grid of dataset W
	//private static AlgorithmCutS algorithmCutS;
	
	private int antidominateAreaCount = 0;
	
	private RTree<Object, Point> tree;
	
	private ArrayList<MyItem> datasetS;
	//private ArrayList<MyItem> datasetW;
	
	private RtopkAlgorithm algorithm;
	
	// setup executed once at the beginning of Reducer
	// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Reducer.html
	@Override
	protected void setup(Reducer<MyKey, MyItem, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		//int gridWSegmentation = context.getConfiguration().getInt("gridWSegmentation", 10);
		
		// initialize k
		k = context.getConfiguration().getInt("K", 0);

		// Initialize the dimensions number of the query
		int queryDimentions = context.getConfiguration().getInt("queryDimentions", 0);
		
		q = new float[queryDimentions];
		
		// add values to the array
		for (int i = 0; i < queryDimentions; i++) {
			q[i] = context.getConfiguration().getFloat("queryDim" + i, 0);
		}
		
		//URI gridWPath = context.getCacheFiles()[1];
		//try {
		//	gridWPath = new URI(new Path(gridWPath.getPath()).getName());
		//} catch (IllegalArgumentException | URISyntaxException e) {
		//	e.printStackTrace();
		//}
		
		/*switch (context.getConfiguration().get("AlgorithmForS")) {
		case "RealBounds":
			algorithmCutS = new AlgorithmS_RealBounds(gridWSegmentation, q);
			break;
		case "Rlists":
			algorithmCutS = new AlgorithmS_Rlists();
			break;
		case "NotRealBounds":
			algorithmCutS = new AlgorithmsS_NotRealBounds(gridWPath, q);
			break;
		case "CompineNotRealBoundsAndRLists":
			algorithmCutS = new AlgorithmS_CombineNotRealBoundsAndRLists(gridWPath, q);
			break;
		default:
			throw new IllegalArgumentException("Algorithm for S is not correct!!!");
		};

		context.setStatus("GridW Created!!!");*/
		//System.out.println(context.getStatus());
		
		String rtopkAlg = "BRS";//context.getConfiguration().get("AlgorithmForRtopk");
		if (rtopkAlg.equals("BRS")) {
			algorithm = RtopkAlgorithm.brs;
			tree = RTree.star().maxChildren(5).create();
		}
		else if (rtopkAlg.equals("RTA")) {
			algorithm = RtopkAlgorithm.rta;
			datasetS = new ArrayList<MyItem>();
			//datasetW = new ArrayList<MyItem>();
		}
		
		
		
	}

	public void reduce(MyKey key, Iterable<MyItem> values, Context context) throws IOException, InterruptedException {
		//RTree tree = new RTree(q.getValues().length);
		
		//algorithmCutS.setReducerKey(key.getKey());
		
		//context.setStatus("Working on grid's W cell: " + key.getKey());
		//System.out.println(context.getStatus());
		
		//long startTime = System.nanoTime();
		
		MyItem myItem;
		
		if (key.getType() == ItemType.W_InTopK) {
			for (MyItem mItem : values) {
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				context.write(new Text(Long.toString(myItem.getId())), myItem.valuesToText());
				context.getCounter(MyCounters.RTOPk_Output).increment(1);
				context.progress();
			}
		}
		else if (key.getType() == ItemType.S_antidom) {
			for (@SuppressWarnings("unused") MyItem mItem : values) {
				antidominateAreaCount++;
				if (antidominateAreaCount >= k) {
					break;
				}
			}
		}
		else if (key.getType() == ItemType.S) {
			//algorithmCutS.setReducerKey(key.getKey());
			for (MyItem mItem : values) {
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				/*if(algorithmCutS.isInLocalAntidominateArea(myItem)){
					antidominateAreaCount++;
					if (antidominateAreaCount >= k) {
						break;
					}
					else {
						continue;
					}
				}*/
				
				if (algorithm == RtopkAlgorithm.brs) {
					tree = tree.add(null, Geometries.point(myItem.values));
				}
				else if (algorithm == RtopkAlgorithm.rta) {
					datasetS.add(myItem);
				}
				context.progress();
			}
		}
		else {
			BrsWithNewTree brs = null;
			Rta rta = null;
			if (algorithm == RtopkAlgorithm.brs) {
				brs = new BrsWithNewTree(k - antidominateAreaCount);		
			}
			else if (algorithm == RtopkAlgorithm.rta) {
				rta = new Rta();
			}
			for (MyItem mItem : values) {
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				//context.getCounter(MyCounters.W2).increment(1);
				//brs = new BrsAlgorithm();
				//long startTime = System.nanoTime();
				if (algorithm == RtopkAlgorithm.brs) {
					if (brs.isWeightVectorInRtopk(q, tree, myItem)) {
						context.write(new Text(Long.toString(myItem.getId())), myItem.valuesToText());
						context.getCounter(MyCounters.RTOPk_Output).increment(1);
					}
				}
				else if (algorithm == RtopkAlgorithm.rta) {
					//datasetW.add(myItem);
					if (rta.isWeightVectorInRtopk(datasetS, myItem, q, k - antidominateAreaCount)) {
						context.write(new Text(Long.toString(myItem.getId())), myItem.valuesToText());
						context.getCounter(MyCounters.RTOPk_Output).increment(1);
					}
				}
				//long estimatedTime = System.nanoTime() - startTime;				
				//context.getCounter(MyCounters.Time_BRS).increment(estimatedTime);
				context.progress();
			}
		}
		
		//long estimatedTime = (System.nanoTime() - startTime) / 1000000000;
		/*switch (key.getType()) {
		case S:
			context.getCounter(MyCounters.Total_effort_to_create_rtree_in_seconds).increment(estimatedTime);
			break;
		case W:
			context.getCounter(MyCounters.Total_effort_for_rtopk_algorithm_in_seconds).increment(estimatedTime);
			break;
		case W_InTopK:
			context.getCounter(MyCounters.Total_effort_for_processing_w_in_rtopk_in_seconds).increment(estimatedTime);
			break;
		}*/
	}
	
	/*
	@Override
	protected void cleanup(Reducer<MyKey, MyItem, Text, Text>.Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		
		if (antidominateAreaCount < k) {
			for (MyItem mItem: datasetS) {
				context.write(null, new Text(mItem.toString()));
			}
			
			for (MyItem mItem: datasetW) {
				context.write(null, new Text(mItem.toString()));
			}
		}
	}*/

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			while (context.nextKey() && antidominateAreaCount < k) {
				reduce(context.getCurrentKey(), context.getValues(), context);
			}
			if (antidominateAreaCount >= k)
				context.getCounter(MyCounters.Reducers_Early_Terminated).increment(1);
		} finally {
			cleanup(context);
		}
	}
}
