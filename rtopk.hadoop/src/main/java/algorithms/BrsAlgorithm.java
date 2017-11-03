package algorithms;

import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;

import generators.UniformGenerator;
import model.MyItem;
import model.RTree;

public class BrsAlgorithm {
	private MyItem weightVector;
	private double queryScore;
	private int k;
	private ArrayList<RTree.Node> buffer;
	
	public BrsAlgorithm(int k) {
		buffer = new ArrayList<>(k);
		this.k = k;
	}
	
	private int getCountOfPointsInNode(RTree.Node n) {
		if (n.isEntry()) {
			if (buffer.contains(n)) {
				return 0;
			}
			else {
				//if (buffer.size() < 2 * k) {
					buffer.add(n);
				//}
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
	
	private int processNode(RTree.Node parent, int max) {
		int count = 0;
		for (RTree.Node child: parent.getChildren()) {
			if (Functions.calculateScore(child.getHigh(), weightVector) < queryScore) {
				count += getCountOfPointsInNode(child);
			}
			else if (Functions.calculateScore(child.getLow(), weightVector) < queryScore) {
				count += processNode(child, max);
				if (count >= max) {
					return count;
				}
			}
		}
		return count;
	}
	/*
	private boolean processNodes(RTree.Node root) {
		int resultCount = 0;
		LinkedList<RTree.Node> children;
		QueueEntries current;
		double bestScore;
		double worstScore;
		queue.add(new QueueEntries(root, 0d));
		
		while ((current = queue.pollFirst()) != null) {
			if (current.getNode().isEntry()) {
				resultCount++;
				if (resultCount >= k) {
					return false;
				}
			}
			else {
				children = current.getNode().getChildren();
				for (int i = 0; i < children.size(); i++) {
					worstScore = Functions.calculateScore(children.get(i).getHigh(), weightVector);
					if (worstScore < queryScore) {
						resultCount += getCountOfPointsInNode(children.get(i));
						if (resultCount >= k) {
							return false;
						}
					}
					else {
						bestScore = Functions.calculateScore(children.get(i).getLow(), weightVector);
						if (bestScore < queryScore) {
							queue.add(new QueueEntries(children.get(i), worstScore));
						}
					}
				}
			}
		}
		
		return resultCount < k;
	}*/
	
	private int checkBuffer() {
		int count = 0;
		for (RTree.Node node: buffer) {
			if (Functions.calculateScore(node.getHigh(), weightVector) < queryScore) {
				count++;
			}
		}
		return count;
	}
	
	public boolean isWeightVectorInRtopk(MyItem queryPoint, RTree tree, MyItem weight) {
		if ((tree.size() == 0) || (k == 0)) {
			return false;
		}
		
		this.weightVector = weight;
		this.queryScore = Functions.calculateScore(queryPoint, weight);
		
		int count = k - checkBuffer();
		if (count <= 0) {
			return false;
		}
		//System.out.println(count);
		return processNode(tree.getRoot(), count) < count;
		//return processNodes(tree.getRoot());
	}
	
	public List<MyItem> computeRTOPk(RTree tree, MyItem[] W, MyItem q) {
		
		List<MyItem> result = new ArrayList<MyItem>();
		for (int i = 0; i < W.length; i++) {
			if (isWeightVectorInRtopk(q, tree, W[i])) {
				result.add(W[i]);
			}
		}
		return result;
	}
	
	public static void main(String[] args) {
		int dimensions = 4;
		int countS = 682503;
		int countW = 1000;
		int k = 10;
		
		RTree tree = new RTree(dimensions);
		com.github.davidmoten.rtree.RTree<Object, Point> newTree = com.github.davidmoten.rtree.RTree.star().create();
		UniformGenerator generator = new UniformGenerator(dimensions);
		MyItem q = new MyItem(new float[]{198878.2f,193482.569f,116926.0f,130030.834f});
		int count = 0;
		MyItem p;
		Point po;
		//ArrayList<MyItem> list = new ArrayList<>();
		while (count < countS) {
			p = new MyItem(generator.nextPoint(10000000));
			if (Dominance.dominate(p.values, q.values) >= 0) {
				po = Geometries.point(p.values);
				tree.insert(p);
				newTree = newTree.add(null, po);
				//list.add(p);
				count++;
			}
		}
		System.out.println("CREATED RTREE!");
		System.out.printf("RTree1 size: %d\n", tree.size());
		System.out.printf("RTree2 size: %d\n", newTree.size());
		System.out.printf("RTree2 depth: %d\n", newTree.calculateDepth());
		//System.out.printf("List size: %d\n", list.size());
		long start = System.currentTimeMillis();
		//tree.sort();
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		System.out.printf("Sorted in %d millis\n", elapsed);
		
		MyItem w;
		
		int count1 = 0, count2 = 0, count3 = 0;
		BrsAlgorithm brs = new BrsAlgorithm(k);
		BrsWithNewTree brs2 = new BrsWithNewTree(k);
		//RtaWithTree rtat = new RtaWithTree();
		start = System.currentTimeMillis();
		for (int i = 0; i < countW; i++) {
			w = new MyItem(generator.nextNormalizedPointF());
			if (brs.isWeightVectorInRtopk(q, tree, w)) {
				count1++;
			}
			if (brs2.isWeightVectorInRtopk(q, newTree, w)) {
				count2++;
			}
			//if (rtat.isWeightVectorInRtopk(tree, w, q.values, k)) {
			//	count3++;
			//}
		}
		end = System.currentTimeMillis();
		elapsed = end - start;
		double msecPerQuery = ((double) elapsed) / (double) countW;
		System.out.printf("Result count1: %d\nResult count2: %d\nResult count3: %d\nMillis elapsed: %d\nmsecPerQuery: %f\n",
				count1, count2, count3, elapsed, msecPerQuery);
		
	}
}
