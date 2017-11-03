package algorithms;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import model.MyItem;

public class Rta {
	private float threshold;
	private S_Item_TopK[] buffer = null;
	
	public boolean isWeightVectorInRtopk(List<MyItem> s, MyItem w, float[] q, int k) {
		if (k == 0) {
			return false;
		}
		else if (s.size() < k) {
			return true;
		}
		
		threshold = Float.MAX_VALUE;
		if (buffer != null) {
			threshold = findBufferMax(buffer, w);
		}
		float scoreq = Functions.calculateScore(w, q);
		if (scoreq < threshold) {
			buffer = TopK(w, s, k, scoreq);
			if (scoreq <= buffer[0].score) {
				return true;
			}
		}
		return false;
	}
	
	public List<MyItem> computeRTOPk(List<MyItem> S, MyItem[] W, float[] q, int k) {
		List<MyItem> Wresults = new ArrayList<MyItem>();
		
		for (int i = 0; i < W.length; i++) {
			if (isWeightVectorInRtopk(S, W[i], q, k)) {
				Wresults.add(W[i]);
			}
		}
		
		return Wresults;
	}
	
	private float findBufferMax(S_Item_TopK[] buffer, MyItem Wi){
		float maxScore = 0;
		float temp;
		for (int i = 0; i < buffer.length; i++) {
			temp = Functions.calculateScore(Wi, buffer[i]);
			if(maxScore < temp)
				maxScore = temp;
		}
		return maxScore;
	}

	private S_Item_TopK[] TopK(MyItem W, List<MyItem> S, int k, float scoreq) {

		PriorityQueue<S_Item_TopK> queue = new PriorityQueue<S_Item_TopK>(k, new myComparator());
		
		float tmpScore;
		for (int i = 0; i < S.size(); i++) {
			tmpScore = Functions.calculateScore(W, S.get(i));
			if (queue.size() < k) {
				S_Item_TopK item = new S_Item_TopK(S.get(i));
				item.score = tmpScore;
				queue.add(item);
				
				if ((queue.size() == k) && (queue.peek().score < scoreq)) {
					break;
				}
			}
			else if (queue.peek().score > tmpScore) {
				S_Item_TopK item = new S_Item_TopK(S.get(i));
				item.score = tmpScore;
				queue.poll();
				queue.add(item);
				
				if (queue.peek().score < scoreq) {
					break;
				}
			}
		}

		S_Item_TopK[] results;
		if (queue.size() < k) {
			results = new S_Item_TopK[queue.size()];
		}
		else {
			results = new S_Item_TopK[k];
		}

		for (int i = 0; i < results.length; i++) {
			results[i] = queue.poll();
		}

		return results;
	}
	

	private class S_Item_TopK extends MyItem {
		private float score;

		private S_Item_TopK(MyItem item) {
			super(item.getId(), item.values);
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
