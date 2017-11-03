package hadoopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import model.AlgorithmType;
import model.DocumentLine;
import model.ItemType;
import model.MyItem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import counters.RTOPkCounters;
import algorithms.BrsWithNewTree;
import algorithms.Dominance;
import algorithms.RTOPk;

import org.apache.hadoop.fs.Path;

public class MyMap extends
		Mapper<LongWritable, DocumentLine, IntWritable, MyItem> {

	private static MyItem q;
	private static int k;
	
	private static int reducersNo;

	private static String fileName_S;
	private static String fileName_W;
	
	private static AlgorithmType algorithmType;

	@Override
	protected void setup(
			Mapper<LongWritable, DocumentLine, IntWritable, MyItem>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		k = context.getConfiguration().getInt("K", 0);
		if (k <= 0)
			throw new IllegalArgumentException("K is not set!!!");
		
		reducersNo = context.getConfiguration().getInt("reducersNo", 1);

		fileName_S = context.getConfiguration().get("FileName_S");

		if (fileName_S == null || fileName_S.trim().equals(""))
			throw new IllegalArgumentException("FileName S is not set!!!");

		fileName_W = context.getConfiguration().get("FileName_W");

		if (fileName_W == null || fileName_W.trim().equals(""))
			throw new IllegalArgumentException("FileName W is not set!!!");

		int queryDimentions = context.getConfiguration().getInt(
				"queryDimentions", 0);

		if (queryDimentions < 1)
			throw new IllegalArgumentException("Query Dimentions is not set!!!");

		float[] queryValues = new float[queryDimentions];

		for (int i = 0; i < queryDimentions; i++) {
			float value = context.getConfiguration().getFloat("queryDim" + i,
					-1);
			if (value < 0)
				throw new IllegalArgumentException("Dimention " + i
						+ " is not set!!!");
			queryValues[i] = value;
		}

		q = new MyItem(queryValues);
		
		algorithmType = AlgorithmType.BRS;
		
		if (algorithmType == AlgorithmType.BRS) {
			tree = RTree.star().create();
		}
		else {
			S = new ArrayList<MyItem>();
		}
		
	}

	private List<MyItem> S;
	private List<MyItem> W = new ArrayList<MyItem>();
	
	private RTree<Object, Point> tree;

	public void map(LongWritable key, DocumentLine value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.getText()
				.toString());
		
		float[] currentItemValues = new float[tokenizer.countTokens() - 1];

		int i = 0;
		long id = 0;
		while (tokenizer.hasMoreTokens()) {
			
			if (i == 0) {
				id = Long.parseLong(tokenizer.nextToken());
			}
			else {
				currentItemValues[i - 1] = Float.parseFloat(tokenizer.nextToken());
			}
			i++;
		}
		if (fileName_S.equals(value.getFile().toString())) {
			
			context.getCounter(RTOPkCounters.TOTAL_S).increment(1);
			if (Dominance.dominate(currentItemValues, q.values) >= 0 || algorithmType==AlgorithmType.NoSortingWNoDominate) {
				
				MyItem item = new MyItem(id, currentItemValues, ItemType.S);
				if (algorithmType == AlgorithmType.BRS) {
					tree = tree.add(null, Geometries.point(item.values));
				}
				else {
					S.add(item);
				}
				for (int i1 = 0; i1 < reducersNo; i1++) {
					context.write(new IntWritable(i1), item);
					context.getCounter(RTOPkCounters.S_output_Map).increment(1);
				}
			}
			else {
				context.getCounter(RTOPkCounters.DOMINACE_REJECTED_S).increment(1);
			}
		}
		else if (fileName_W.equals(value.getFile().toString())) {
			W.add(new MyItem(id, currentItemValues, ItemType.W));
			context.getCounter(RTOPkCounters.TOTAL_W).increment(1);
		}
		else {
			throw new IllegalArgumentException("Wrong Filenames!!!");
		}

	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, DocumentLine, IntWritable, MyItem>.Context context) throws IOException, InterruptedException {
			
		List<MyItem> results_RTOPk = null;
		
		if (algorithmType == AlgorithmType.BRS) {
			BrsWithNewTree brs = new BrsWithNewTree(k);
			results_RTOPk = brs.computeRTOPk(tree, W.toArray(new MyItem[0]), q);
		}
		else {
			RTOPk rtopK = new RTOPk(algorithmType);
			results_RTOPk = rtopK.computeRTOPk(S.toArray(new MyItem[0]), W.toArray(new MyItem[0]), q, k);
		}
		
		context.getCounter(RTOPkCounters.W_reject_Map).increment(W.size() - results_RTOPk.size());

		int count = 0;
		for (MyItem item : results_RTOPk) {
			context.write(new IntWritable(count++ % reducersNo), item);
		}
		/*}
		// �� �� �������� S ����� �������� ��� �, ����...
		else {
			// ��������� ���� Reducer ��� �� W ����� �� q �� ����� �� ��� �� TopK
			int count = 0;
			for (MyItem item : W){
				context.write(new IntWritable(count++ % reducersNo), item);
				//��������� ��� counter W_output_Map ���� 1
				context.getCounter(RTOPkCounters.W_output_Map).increment(1);
			}
		}*/

		super.cleanup(context);
	}

}
