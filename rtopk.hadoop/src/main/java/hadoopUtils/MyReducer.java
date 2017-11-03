package hadoopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import model.AlgorithmType;
import model.ItemType;
import model.MyItem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import counters.RTOPkCounters;
import algorithms.BrsWithNewTree;
import algorithms.Rta;

public class MyReducer extends Reducer<IntWritable, MyItem, Text, Text> {

	private static MyItem q;
	private static int k;

	private static AlgorithmType algorithmType;

	@Override
	protected void setup(Reducer<IntWritable, MyItem, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		k = context.getConfiguration().getInt("K", 0);
		if (k <= 0)
			throw new IllegalArgumentException("K is not set!!!");

		int queryDimentions = context.getConfiguration().getInt("queryDimentions", 0);

		if (queryDimentions < 1)
			throw new IllegalArgumentException("Query Dimentions is not set!!!");

		float[] queryValues = new float[queryDimentions];

		for (int i = 0; i < queryDimentions; i++) {
			float value = context.getConfiguration().getFloat("queryDim" + i, -1);
			if (value < 0)
				throw new IllegalArgumentException("Dimention " + i + " is not set!!!");
			queryValues[i] = value;
		}

		q = new MyItem(0, queryValues, ItemType.S);

		algorithmType = AlgorithmType.BRS; //context.getConfiguration().getEnum("algorithmType", AlgorithmType.FULL);
	}

	public void reduce(IntWritable key, Iterable<MyItem> values, Context context)
			throws IOException, InterruptedException {
		List<MyItem> S = null;
		RTree<Object, Point> tree = null;
		if (algorithmType == AlgorithmType.BRS) {
			tree = RTree.star().create();
		} else {
			S = new ArrayList<MyItem>();
		}
		List<MyItem> W = new ArrayList<MyItem>();

		MyItem temp;

		for (MyItem item : values) {
			temp = new MyItem(item);

			if (temp.getItemType() == ItemType.S) {
				if (algorithmType == AlgorithmType.BRS) {
					tree = tree.add(null, Geometries.point(temp.values));
				} else {
					S.add(temp);
				}
			}
			else {
				context.getCounter(RTOPkCounters.W_Input_Reducer).increment(1);
				W.add(temp);

				/*
				 * boolean success; if (algorithmType == AlgorithmType.BRS) {
				 * success = brsRtopK.isWeightVectorInRtopk(q, tree, temp, k); }
				 * else { success = rtaRtopK.isWeightVectorInRtopk(S, temp,
				 * q.values, k); }
				 * 
				 * if (success) { context.write(new Text(item.getId() + ""),
				 * item.valuesToText()); // ������ ��� ������� W_output_Reducer
				 * ���� 1
				 * context.getCounter(RTOPkCounters.W_output_Reducer).increment(
				 * 1); }
				 */
			}
		}

		BrsWithNewTree brsRtopK = null;
		Rta rtaRtopK = null;
		if (algorithmType != AlgorithmType.BRS) {
			rtaRtopK = new Rta();
		}
		
		boolean success;
		brsRtopK = new BrsWithNewTree(k);
		for (MyItem w : W) {
			if (algorithmType == AlgorithmType.BRS) {
				success = brsRtopK.isWeightVectorInRtopk(q, tree, w);
			} else {
				success = rtaRtopK.isWeightVectorInRtopk(S, w, q.values, k);
			}

			if (success) {
				context.write(new Text(w.getId() + ""), w.valuesToText());
				context.getCounter(RTOPkCounters.W_output_Reducer).increment(1);
			}
		}

		// �� �� �������� ��� ������ S ����� ����������� � ��� �� �� �, ����...
		// if (S.size() >= k) {

		// ������ �� ���������� �� ������������ ��� RTOPk

		/*
		 * } // ������ �� �� �������� ��� ������ S ����� �������� ��� �, ����...
		 * else { // ������ �� ���������� ��� �� �������� ��� ������ W // �����
		 * �� K �� ������ ����� �� ��� �� TopK for (MyItem item : W) {
		 * context.write( new Text(item.getItemType() + " - " + item.getId()),
		 * item.valuesToText()); // ������ ��� ������� W_output_Reducer ���� 1
		 * context.getCounter(RTOPkCounters.W_output_Reducer).increment(1); } }
		 */
	}

}
