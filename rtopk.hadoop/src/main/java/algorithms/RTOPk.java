package algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import hadoopUtils.MyComparatorForW;
import model.AlgorithmType;
import model.MyItem;

public class RTOPk {

	//private Counter counterRTOPk_W;
	//private Counter counterExamine_WTopK;
	private AlgorithmType algorithmType;
	
	//private int counter = 0;
	
	/**
	 * 
	 * @param counterExamine_WTopK �������� ��� �� ������ ��� W ��������� ��� ���������� �� TopK ����
	 * @param counterRTOPk_W �������� ��� �� ������ ��� W ��� ��������� ��� RTOPk
	 */
	public RTOPk(/*Counter counterExamine_WTopK,Counter counterRTOPk_W, */AlgorithmType algorithmType){
		//this.counterExamine_WTopK = counterExamine_WTopK;
		//this.counterRTOPk_W = counterRTOPk_W;
		this.algorithmType = algorithmType;
	}
	
	/**
	 * <h1>���������� �� RTOPk �� ��� ��������� RTA</h1>
	 * 
	 * � ������� ���� ���������� ������� ��� Reverce Top-K ���������� ���
	 * ���������� �� W ��� �� ����� ��� Top-K ���� �� ��������� q ������
	 * 
	 * @param S
	 *            ��������
	 * @param W
	 *            ����������� ������
	 * @param q
	 *            query
	 * @param k
	 *            ��������� �� Top-K
	 * @return ����� �� ���� ������� ��� �� q ������ �� ���� ��� Top-K ����
	 */
	public List<MyItem> computeRTOPk(MyItem[] S, MyItem[] W, MyItem q, int k) {
		
		if(algorithmType==AlgorithmType.FULL){
			MyComparatorForW myComparator = new MyComparatorForW(q.getValues().length,0.5);
	
			for (int i = 0; i < W.length; i++) {
				W[i].calculateCosine();
			}
			// ������������� �� W ���� ��� Query ���� �� �������� �������� � RTA
			// ����������
			Arrays.sort(new MyItem[0], myComparator);
			//W.sort(myComparator);
		}
		
		//System.out.println("Finished sort");
		
		List<MyItem> Wresults = new ArrayList<MyItem>();

		Double threshold = Double.MAX_VALUE;
		S_Item_TopK[] buffer = null;
		
		double scoreq;
		for (int i = 0; i < W.length; i++) {
			scoreq = fscore(W[i], q);
			if (scoreq <= threshold) {
				buffer = TopK(W[i], S, k);
				if (scoreq <= buffer[0].score) {
					Wresults.add(W[i]);
					//��������� ��� counter RTOPk_W ���� 1
					//counterRTOPk_W.increment(1);
				}
				//��������� ��� counter Examine_WTopK ���� 1
				//counterExamine_WTopK.increment(1);
			}
			//else {
			//	counter++;
			//}
			
			if (i + 1 < W.length)
				threshold = findBufferMax(buffer,W[i + 1]);
		}

		
		return Wresults;
	}
	
	private double findBufferMax(S_Item_TopK[] buffer, MyItem Wi){
		double maxScore = 0;
		double temp;
		for (int i = 0; i < buffer.length; i++) {
			temp = fscore(Wi, buffer[i]);
			if(maxScore < temp)
				maxScore = temp;
		}
		return maxScore;
	}
	
	/**
	 * <h1>���������� �� score</h1>
	 * 
	 * � ������� ���� ���������� �� score ��� ���� ��� p ������ ��� ���� W
	 * ������. ��� ��������� ����� �� score ���� �������� (���. ���������
	 * ����������� ���� ����������� ��� ������).
	 * 
	 * @param W
	 *            ����������� ������
	 * @param p
	 *            ��������
	 * @return ����������
	 */
	public static double fscore(MyItem W, MyItem p) {

		//if (W.values.length != p.values.length)
		//	throw new IllegalArgumentException(
		//			"Must have the same dimentions in order to calculate f!!!");

		double score = 0;

		for (int i = 0; i < W.values.length; i++) {
			score += W.values[i] * p.values[i];
		}

		return score;
	}

	/**
	 * 
	 * 
	 * @param W
	 *            ����������� ������
	 * @param S
	 *            ��������
	 * @param k
	 *            ������� Top-K
	 * @return ������������ ����� �� Top-K �������� ��� ��� ������ W
	 */
	private S_Item_TopK[] TopK(MyItem W, MyItem[] S, int k) {

		PriorityQueue<S_Item_TopK> queue = new PriorityQueue<S_Item_TopK>(k, new myComparator());

		// �������� �� �������� ���� ����� �� ��� score ��� �� ���� ��������
		double tmpScore;
		for (int i = 0; i < S.length; i++) {
			//S_Item_TopK item = new S_Item_TopK(Si);
			//item.score = fscore(W, Si);
			tmpScore = fscore(W, S[i]);
			if (queue.size() < k) {
				S_Item_TopK item = new S_Item_TopK(S[i]);
				item.score = tmpScore;
				queue.add(item);
			}
			else if (queue.peek().score > tmpScore) {
				S_Item_TopK item = new S_Item_TopK(S[i]);
				item.score = tmpScore;
				queue.poll();
				queue.add(item);
			}
		}

		S_Item_TopK[] results = new S_Item_TopK[k];

		// ��������� �� K ����� �������� (�� �� ��������� score)
		for (int i = 0; i < k; i++) {
			results[i] = queue.poll();
		}

		return results;
	}
	/*
	private double[] convertPoint(float[] point) {
		double[] result = new double[point.length];
		for (int i = 0; i < point.length; i++) {
			result[i] = point[i];
		}
		return result;
	}
	
	private MyItem[] generateDatasetS() {
		List<MyItem> result = new ArrayList<MyItem>();
		UniformGenerator generator = new UniformGenerator(2);
		for (int i = 0; i < 22000; i++) {
			result.add(new MyItem("" + i, convertPoint(generator.nextPoint(10000)), ItemType.S));
		}
		return result.toArray(new MyItem[0]);
	}
	
	private MyItem[] generateDatasetW() {
		List<MyItem> result = new ArrayList<MyItem>();
		UniformGenerator generator = new UniformGenerator(2);
		for (int i = 0; i < 22000; i++) {
			result.add(new MyItem("" + i, convertPoint(generator.nextNormalizedPoint()), ItemType.W));
		}
		return result.toArray(new MyItem[0]);
	}
	
	private MyItem generateQuery() {
		UniformGenerator generator = new UniformGenerator(2);
		return new MyItem("0", convertPoint(generator.nextPoint(100)), ItemType.S);
	}
	
	private void test() {
		MyItem[] S = generateDatasetS();
		MyItem[] W = generateDatasetW();
		MyItem q = generateQuery();
		
		List<MyItem> rtopk = computeRTOPk(S, W, q, 5);
		for (int i = 0; i < rtopk.size(); i++) {
			System.out.println(rtopk.get(i));
		}
		System.out.println(rtopk.size());
		//System.out.println(counter);
		
		//counter = 0;
		this.algorithmType = AlgorithmType.FULL;
		rtopk = computeRTOPk(S, W, q, 5);
		for (int i = 0; i < rtopk.size(); i++) {
			System.out.println(rtopk.get(i));
		}
		System.out.println(rtopk.size());
		//System.out.println(counter);
	}
	
	public static void main(String[] args) {
		new RTOPk(AlgorithmType.NoSortingW).test();
	}*/

	private class S_Item_TopK extends MyItem {
		private double score;

		private S_Item_TopK(MyItem item) {
			super(item.getId(), item.values, item.getItemType());
		}

	}

	private class myComparator implements Comparator<S_Item_TopK> {
		@Override
		public int compare(S_Item_TopK a, S_Item_TopK b) {
			if (a.score > b.score)
				return -1;
			else if (a.score < b.score)
				return 1;
			else
				return 0;
		}
	}

}
