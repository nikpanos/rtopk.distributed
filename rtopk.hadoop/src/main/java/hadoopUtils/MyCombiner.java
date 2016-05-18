package hadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import algorithms.cutS.AlgorithmS_Rlists;
import hadoopUtils.counters.MyCounters;
import model.ItemType;
import model.MyItem;
import model.MyKey;

public class MyCombiner extends Reducer<MyKey, MyItem, MyKey, MyItem> {
	
	// The query
	private static float[] q;
	// The K of RTOPk
	private static int k;

	// The grid of dataset W
	private static AlgorithmS_Rlists algorithmCutS;
	
	private int antidominateAreaCount = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// initialize k
				k = context.getConfiguration().getInt("K", 0);
				if (k <= 0)
					throw new IllegalArgumentException("K is not set!!!");

				// Initialize the dimensions number of the query
				int queryDimentions = context.getConfiguration().getInt(
						"queryDimentions", 0);

				if (queryDimentions < 1)
					throw new IllegalArgumentException("Query Dimentions is not set!!!");
				
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
				
				URI gridWPath = context.getCacheFiles()[1];
				try {
					gridWPath = new URI(new Path(gridWPath.getPath()).getName());
				} catch (IllegalArgumentException | URISyntaxException e) {
					e.printStackTrace();
				}
				
				algorithmCutS = new AlgorithmS_Rlists(k, gridWPath, q);

				context.setStatus("GridW Created!!!");
				//System.out.println(context.getStatus());
				
				
	}
	
	public void reduce(MyKey key, Iterable<MyItem> values, Context context) throws IOException, InterruptedException {
		//RTree tree = new RTree(q.getValues().length);
		
		algorithmCutS.setReducerKey(key.getKey());
		
		context.setStatus("Working on grid's W cell: " + key.getKey());
		//System.out.println(context.getStatus());
		
		//long startTime = System.nanoTime();
		
		MyItem myItem;
		
		if (key.getType() == ItemType.W_InTopK) {
			for (MyItem mItem : values) {
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				context.write(key, myItem);
				context.progress();
			}
		}
		else if (antidominateAreaCount >= k) {
			return;
		}
		else if (key.getType() == ItemType.S) {
			for (MyItem mItem : values) {
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				//context.getCounter(MyCounters.S2).increment(1);
				if(algorithmCutS.isInLocalAntidominateArea(myItem)){
					antidominateAreaCount++;
					if (antidominateAreaCount >= k) {
						context.getCounter(MyCounters.Combiners_Early_Terminated).increment(1);
						context.write(key, myItem);
						return;
					}
				}
				
				if (algorithmCutS.checkPointUsingList(myItem)) {
					context.getCounter(MyCounters.S2_by_combiner).increment(1);
					context.write(key, myItem);
				}
				else {
					context.getCounter(MyCounters.S2_pruned_by_RLists_in_Combiner).increment(1);
				}
				//long estimatedTime = System.nanoTime() - startTime;				
				//context.getCounter(MyCounters.TimeToCreate_RTree).increment(estimatedTime);
				context.progress();
			}
		}
		else {
			for (MyItem mItem : values) {
				context.getCounter(MyCounters.W2).increment(1);
				myItem = new MyItem(mItem.getId(), mItem.getValues().clone());
				context.write(key, myItem);
				context.progress();
			}
		}
	}
}
