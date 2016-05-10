package algorithms;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import model.MyItem;
import model.QueueEntries;
import model.RTree;
import model.RTreeNodesComparator;

public class Brs {
	private MyItem weightVector;
	private double queryScore;
	private int k;
	private TreeSet<QueueEntries> queue;
	
	private boolean processNodes(RTree.Node parent) {
		//MyItem[] result = new MyItem[k];
		
		int resultCount = 0;
		LinkedList<RTree.Node> children;
		QueueEntries best;
		children = parent.getChildren();
		float currentScore;
		for (int i = 0; i < children.size(); i++) {
			currentScore = Functions.calculateScore(children.get(i).getLow(), weightVector);
			if (currentScore < queryScore) {
				queue.add(new QueueEntries(children.get(i), currentScore));
			}
		}
		children = null;  //Helps the GC
		
		best = queue.pollFirst();
		while ((best != null) && (resultCount < k)) {
			if (best.getNode() instanceof RTree.Entry) {
				//RTree.Entry e = (RTree.Entry) best.getNode();
				
				resultCount++;
				if (queryScore < best.getScore()) {
					return true;
				}
			}
			else {
				children = best.getNode().getChildren();
				for (int i = 0; i < children.size(); i++) {
					currentScore = Functions.calculateScore(children.get(i).getLow(), weightVector);
					if (currentScore < queryScore) {
						queue.add(new QueueEntries(children.get(i), currentScore));
					}
				}
				children = null;  //Helps the GC
			}
			best = queue.pollFirst();
		}
		
		return resultCount < k;
	}
	
	public boolean isWeightVectorInRtopk(float[] queryPoint, RTree tree, MyItem weight, int k) {
		if ((tree.size() == 0) || (k == 0)) {
			return false;
		}
		
		this.weightVector = weight;
		this.k = k;
		this.queryScore = Functions.calculateScore(queryPoint, weight);
		this.queue = new TreeSet<QueueEntries>(new RTreeNodesComparator());
		
		return processNodes(tree.getRoot());
	}
	
	public List<MyItem> computeRTOPk(RTree tree, MyItem[] W, float[] q, int k) {
		
		List<MyItem> result = new ArrayList<MyItem>();
		for (int i = 0; i < W.length; i++) {
			if (isWeightVectorInRtopk(q, tree, W[i], k)) {
				result.add(W[i]);
			}
		}
		return result;
	}
}
