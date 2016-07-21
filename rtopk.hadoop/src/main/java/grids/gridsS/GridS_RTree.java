package grids.gridsS;

import java.io.IOException;
import java.net.URI;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;

import algorithms.Dominance;
import algorithms.FileParser;
import algorithms.Functions;
import model.Cell_S;
import model.MyItem;

public class GridS_RTree extends GridS {
	
	private float[] q;
	private RTree<Integer, Rectangle> tree;
	private int antidominateAreaCount = 0;
	
	public GridS_RTree(URI gridSPath,float[] query) throws IOException {
		super();
		this.q = query;
		tree = RTree.star().create();
		FileParser.parseGridSFile(gridSPath, this);
	}

	@Override
	public void add(Cell_S cell) {
		if (Dominance.dominateBoostQuery(q, cell.getUpperBound()) == -1) {
			antidominateAreaCount += cell.getCount();
		}
		else if (Dominance.dominateBoostQuery(cell.getLowerBound(), q) != -1) {
			tree = tree.add(cell.getCount(), Geometries.rectangle(cell.getLowerBound(), cell.getUpperBound()));
		}
	}
	
	private int getAllCount(Node<Integer, Rectangle> node) {
		int result = 0;
		if (node instanceof Leaf) {
			Leaf<Integer, Rectangle> l = (Leaf<Integer, Rectangle>)node;
			for (Entry<Integer, Rectangle> e: l.entries()) {
				result += e.value();
			}
		}
		else {
			NonLeaf<Integer, Rectangle> l = (NonLeaf<Integer, Rectangle>)node;
			for (Node<Integer, Rectangle> child: l.children()) {
				result += getAllCount(child);
			}
		}
		return result;
	}
	
	private int[] getCountOfNode(MyItem w, Node<Integer, Rectangle> node, float scoreQ) {
		//float tmpScore;
		int result[] = new int[]{0, 0};
		if (node instanceof Leaf) {
			Leaf<Integer, Rectangle> l = (Leaf<Integer, Rectangle>)node;
			for (Entry<Integer, Rectangle> e: l.entries()) {
				//tmpScore = ;
				if (Functions.calculateScore(w, e.geometry().high()) <= scoreQ) {
					result[0] += e.value();
					result[1] += e.value();
				}
				else if (Functions.calculateScore(w, e.geometry().low()) < scoreQ) {
					result[1] += e.value();
				}
			}
		}
		else {
			NonLeaf<Integer, Rectangle> l = (NonLeaf<Integer, Rectangle>)node;
			for (Node<Integer, Rectangle> child: l.children()) {
				if (Functions.calculateScore(w, child.geometry().mbr().high()) <= scoreQ) {
					int count = getAllCount(child);
					result[0] += count;
					result[1] += count;
				}
				else if (Functions.calculateScore(w, child.geometry().mbr().low()) < scoreQ) {
					int[] counts = getCountOfNode(w, child, scoreQ);
					result[0] += counts[0];
					result[1] += counts[1];
				}
			}
		}
		return result;
	}

	@Override
	public int[] getCount(MyItem w, float[] query) {
		float scoreQ = Functions.calculateScore(w, query);
		int[] result = getCountOfNode(w, tree.root().get(), scoreQ);
		result[0] += antidominateAreaCount;
		result[1] += antidominateAreaCount;
		return result;
	}

	@Override
	public int getAntidominateAreaCount(float[] query) {
		return antidominateAreaCount;
	}

}
