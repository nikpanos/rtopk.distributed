package algorithms;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import model.MyItem;
import model.RTree;

public class RtaWithTree {
	
	private float scoreQ;
	private ArrayList<RTree.Node> buffer;
	
	public RtaWithTree() {
		buffer = new ArrayList<>();
	}
	
	private int getCountOfPointsInNode(RTree.Node n) {
		if (n.isEntry()) {
			if (buffer.contains(n)) {
				return 0;
			}
			else {
				buffer.add(n);
				return 1;
			}
		}
		else {
			int result = 0;
			for (RTree.Node child: n.getChildren()) {
				result += getCountOfPointsInNode(child);
			}
			return result;
		}
	}
	
	private int topk(RTree tree, MyItem w, int k) {
		PriorityQueue<NodeWithScore> queue = new PriorityQueue<>(new Comparator<NodeWithScore>() {
			@Override
			public int compare(NodeWithScore o1, NodeWithScore o2) {
				return (o1.score < o2.score) ? -1 : ((o1.score == o2.score) ? 0 : 1);
			}
		});
		queue.add(new NodeWithScore(tree.getRoot(), 0f));
		
		NodeWithScore current;
		int count = 0;
		
		while ((current = queue.poll()) != null) {
			if (current.node.isEntry()) {
				if (current.score > scoreQ) {
					return count;
				}
				else if (!buffer.contains(current.node)) {
					buffer.add(current.node);
					if (++count >= k) {
						break;
					}
				}
			}
			else {
				if (current.node.isLeaf()) {
					for (RTree.Node child: current.node.getChildren()) {
						if (!buffer.contains(child)) {
							float score = Functions.calculateScore(w.values, child.getLow().values);
							queue.add(new NodeWithScore(child, score));
						}
					}
				}
				else {
					for (RTree.Node child: current.node.getChildren()) {
						if (Functions.calculateScore(w.values, child.getHigh().values) < scoreQ) {
							count += getCountOfPointsInNode(child);
							if (count >= k) {
								return count;
							}
						}
						else {
							float score = Functions.calculateScore(w.values, child.getLow().values);
							if (score < scoreQ) {
								queue.add(new NodeWithScore(child, score));
							}
						}
					}
				}
			}
		}
		
		return count;
	}
	
	private int countInBuffer(MyItem w) {
		int result = 0;
		for (RTree.Node n: buffer) {
			if (Functions.calculateScore(w.values, n.getLow().values) < scoreQ) {
				result++;
			}
		}
		return result;
	}
	
	public boolean isWeightVectorInRtopk(RTree tree, MyItem w, float[] q, int k) {
		scoreQ = Functions.calculateScore(w.values, q);
		int count = k - countInBuffer(w);
		return topk(tree, w, count) < count;
	}
	
	public class NodeWithScore {
		private RTree.Node node;
		private float score;
		
		public NodeWithScore(RTree.Node node, float score) {
			this.node = node;
			this.score = score;
		}

		public RTree.Node getNode() {
			return node;
		}

		public float getScore() {
			return score;
		}
	}
}
