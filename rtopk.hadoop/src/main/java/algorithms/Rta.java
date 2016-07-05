package algorithms;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import model.MyItem;
import model.MyKey;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Rta {
	private float threshold;
	private S_Item_TopK[] buffer = null;
	//private boolean isInterestingWeight = false;
	
	private boolean vectorsMatch(float[] vectorA, float[] vectorB) {
		for (int i = 0; i < vectorA.length; i++) {
			if (vectorA[i] != vectorB[i]) {
				return false;
			}
		}
		return true;
	}
	
	public boolean isWeightVectorInRtopk(List<MyItem> s, MyItem w, float[] q, int k, Reducer<MyKey, MyItem, Text, Text>.Context context) {
		FileSystem fs = null;
		BufferedWriter br = null;
		boolean isInterestingWeight = false;
		
		if (vectorsMatch(w.values, new float[]{0.6702f, 0.0364f, 0.1304f, 0.163f})) {
			isInterestingWeight = true;
			try {
				fs = FileSystem.get(context.getConfiguration());
				Path filepath = new Path(FileOutputFormat.getOutputPath(context).toString() + "/debug.txt");
				if (!fs.exists(filepath)) {
					br = new BufferedWriter(new OutputStreamWriter(fs.create(filepath), "UTF-8"));
				}
				else {
					isInterestingWeight = false;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			threshold = Float.MAX_VALUE;
			if (buffer != null) {
				if (isInterestingWeight) {
					br.write("Buffer is not null\n");
					br.write("Buffer length: " + buffer.length + "\n");
				}
				threshold = findBufferMax(buffer, w);
				if (isInterestingWeight) {
					br.write("threshold set to: " + threshold + "\n");
				}
			}
			float scoreq = Functions.calculateScore(w, q);
			if (isInterestingWeight) {
				br.write("scoreq set to: " + scoreq + "\n");
			}
			if (scoreq <= threshold) {
				if (isInterestingWeight) {
					br.write("scoreq <= threshold : true\n");
				}
				buffer = TopK(w, s, k);
				if (isInterestingWeight) {
					br.write("buffer size: " + buffer.length + "\n");
					for (int i = 0; i < buffer.length; i++) {
						br.write(i + "\t" +  buffer[i].valuesToText().toString() + "\n");
						br.write("buffer[" + i + "].score: " + buffer[i].score + "\n");
					}
					br.write("end of buffer\n");
				}
				if (scoreq <= buffer[0].score) {
					if (isInterestingWeight) {
						br.write("scoreq <= buffer[0].score : true\n");
					}
					isInterestingWeight = false;
					return true;
				}
				else if (isInterestingWeight) {
					br.write("scoreq > buffer[0].score : true\n");
				}
			}
			else if (isInterestingWeight) {
				br.write("scoreq <= threshold : false\n");
				for (int i = 0; i < buffer.length; i++) {
					br.write(i + "\t" +  buffer[i].valuesToText().toString() + "\n");
					br.write("buffer[" + i + "].score: " + buffer[i].score + "\n");
				}
				br.write("end of buffer\n");
			}
			isInterestingWeight = false;
			return false;
		}
		catch (IOException ex) {
			return false;
		}
		finally {
			isInterestingWeight = false;
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	/*
	public List<MyItem> computeRTOPk(List<MyItem> S, MyItem[] W, float[] q, int k) {
		List<MyItem> Wresults = new ArrayList<MyItem>();
		
		for (int i = 0; i < W.length; i++) {
			if (isWeightVectorInRtopk(S, W[i], q, k)) {
				Wresults.add(W[i]);
			}
		}
		
		return Wresults;
	}*/
	
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

	private S_Item_TopK[] TopK(MyItem W, List<MyItem> S, int k) {

		PriorityQueue<S_Item_TopK> queue = new PriorityQueue<S_Item_TopK>(k, new myComparator());
		
		float tmpScore;
		for (int i = 0; i < S.size(); i++) {
			tmpScore = Functions.calculateScore(W, S.get(i));
			if (queue.size() < k) {
				S_Item_TopK item = new S_Item_TopK(S.get(i));
				item.score = tmpScore;
				queue.add(item);
				
				/*if ((queue.size() == k) && (queue.peek().score < scoreq)) {
					break;
				}*/
			}
			else if (queue.peek().score > tmpScore) {
				S_Item_TopK item = new S_Item_TopK(S.get(i));
				item.score = tmpScore;
				queue.poll();
				queue.add(item);
				
				/*if (queue.peek().score < scoreq) {
					break;
				}*/
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
