package algorithms;

import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Point;

import model.MyItem;

public class BrsWithNewTree {
	private MyItem weightVector;
	private double queryScore;
	private int k;
	private ArrayList<Point> buffer;
	
	public BrsWithNewTree(int k) {
		buffer = new ArrayList<Point>(k);
		this.k = k;
	}
	
	private int processEntry(Entry<Object, Point> e) {
		if (buffer.contains(e.geometry())) {
			return 0;
		}
		else {
			//if (buffer.size() < 2 * k) {
				buffer.add(e.geometry());
			//}
			return 1;
		}
	}
	
	private int getCountOfPointsInNode(Node<Object, Point> n) {
		if (n instanceof Leaf) {
			int result = 0;
			List<Entry<Object, Point>> entries = ((Leaf<Object, Point>)n).entries();
			for (Entry<Object, Point> entry: entries) {
				result += processEntry(entry);
			}
			return result;
		}
		else {
			NonLeaf<Object, Point> nodes = (NonLeaf<Object, Point>)n;
			int result = 0;
			for (Node<Object, Point> child: nodes.children()) {
				result += getCountOfPointsInNode(child);
			}
			return result;
		}
	}
	
	private int processNode(Node<Object, Point> parent, int max) {
		int count = 0;
		if (parent instanceof NonLeaf) {
			NonLeaf<Object, Point> pNode = (NonLeaf<Object, Point>) parent;
			for (Node<Object, Point> child: pNode.children()) {
				if (Functions.calculateScore(child.geometry().mbr().high(), weightVector.values) < queryScore) {
					count += getCountOfPointsInNode(child);
				}
				else if (Functions.calculateScore(child.geometry().mbr().low(), weightVector.values) < queryScore) {
					count += processNode(child, max);
					if (count >= max) {
						return count;
					}
				}
			}
		}
		else {
			Leaf<Object, Point> lNode = (Leaf<Object, Point>) parent;
			for (Entry<Object, Point> child: lNode.entries()) {
				if (Functions.calculateScore(child.geometry().low(), weightVector.values) < queryScore) {
					count += processEntry(child);
				}
			}
		}
		return count;
		
		//return count;
	}
	
	private int checkBuffer() {
		int count = 0;
		for (Point p: buffer) {
			if (Functions.calculateScore(p.low(), weightVector.values) < queryScore) {
				count++;
			}
		}
		return count;
	}
	
	public boolean isWeightVectorInRtopk(MyItem queryPoint, RTree<Object, Point> tree, MyItem weight) {
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
		return processNode(tree.root().get(), count) < count;
		//return processNodes(tree.getRoot());
	}
	
	public List<MyItem> computeRTOPk(RTree<Object, Point> tree, MyItem[] W, MyItem q) {
		
		List<MyItem> result = new ArrayList<MyItem>();
		for (int i = 0; i < W.length; i++) {
			if (isWeightVectorInRtopk(q, tree, W[i])) {
				result.add(W[i]);
			}
		}
		return result;
	}
}
