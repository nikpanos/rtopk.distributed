package grids.gridsS;

import java.io.BufferedReader;
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
	
	protected float[] q;
	protected int k;
	protected RTree<Integer, Rectangle> tree;
	protected int antidominateAreaCount = 0;
	
	//private long cell_count = 0;
	//private long used_cell_count = 0;
	
	//private long cells_checked_count = 0;
	//private long comparisons_count = 0;
	
	public GridS_RTree(URI gridSPath, float[] query, int k) throws IOException {
		super();
		this.q = query;
		tree = RTree.star().create();
		this.k = k;
		FileParser.parseGridSFile(gridSPath, this);
	}
	
	public GridS_RTree(BufferedReader gridReader, float[] query, int k) throws IOException {
		super();
		this.q = query;
		tree = RTree.star().create();
		this.k = k;
		FileParser.parseGridSFile(gridReader, this);
	}

	@Override
	public void add(Cell_S cell) {
		//cell_count++;
		if (Dominance.dominateBoostQuery(q, cell.getUpperBound()) == -1) {
			antidominateAreaCount += cell.getCount();
		}
		else if (Dominance.dominateBoostQuery(cell.getLowerBound(), q) != -1) {
			//used_cell_count++;
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
				//cells_checked_count++;
				//comparisons_count++;
				if (Functions.calculateScore(w, e.geometry().high()) <= scoreQ) {
					result[0] += e.value();
					result[1] += e.value();
				}
				else {
					//comparisons_count++;
					if (Functions.calculateScore(w, e.geometry().low()) < scoreQ) {
						result[1] += e.value();
					}
				}
				if (result[0] >= k) {
					break;
				}
			}
		}
		else {
			NonLeaf<Integer, Rectangle> l = (NonLeaf<Integer, Rectangle>)node;
			for (Node<Integer, Rectangle> child: l.children()) {
				//cells_checked_count++;
				//comparisons_count++;
				if (Functions.calculateScore(w, child.geometry().mbr().high()) <= scoreQ) {
					int count = getAllCount(child);
					result[0] += count;
					result[1] += count;
				}
				else {
					//comparisons_count++;
					if (Functions.calculateScore(w, child.geometry().mbr().low()) < scoreQ) {
						int[] counts = getCountOfNode(w, child, scoreQ);
						result[0] += counts[0];
						result[1] += counts[1];
					}
				}
				if (result[0] >= k) {
					break;
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
	
	public static void main(String[] args) throws IOException {
		/*int k = 500;
		float[] query = new float[] {52616, 52190, 52566, 52383};
		String filenameGrid = "C:\\Users\\nikp\\Desktop\\100Suni4.grid5";
		String filenameW = "C:\\Users\\nikp\\Desktop\\part-m-00098";
		int prunedCount = 0, inRtopkCount = 0, otherCount = 0;
		
		long timeStart = System.currentTimeMillis(), timeRead;
		
		try (BufferedReader reader = new BufferedReader(new FileReader(filenameGrid))) {
			GridS_RTree tree = new GridS_RTree(reader, query, k);
			timeRead = System.currentTimeMillis();
			
			try (BufferedReader readerW = new BufferedReader(new FileReader(filenameW))) {
				String line;
				MyItem w;
				while ((line = readerW.readLine()) != null) {
					w = FileParser.parseDatasetElement(line);
					int[] range = tree.getCount(w, query);
					if(k < range[0])
						prunedCount++;
					else if(range[1] < k) {
						inRtopkCount++;
					}
					else {
						otherCount++;
					}
				}
			}
			System.out.printf("Cells in file: %d\nCells added in RTree: %d\nCells checked %d\n", tree.cell_count, tree.used_cell_count, tree.cells_checked_count);
			System.out.printf("Comparisons count: %d\n", tree.comparisons_count);
		}
		long timeEnd = System.currentTimeMillis();
		
		
		System.out.printf("Total execution time: %dms\n", timeEnd - timeStart);
		System.out.printf("Time for creating RTree: %dms\n", timeRead - timeStart);
		System.out.printf("Time for processing w tuples: %dms\n", timeEnd - timeRead);
		System.out.printf("Tuples pruned: %d\nTuples in RTOPK: %d\nOther tuples: %d\n", prunedCount, inRtopkCount, otherCount);
		*/
	}
}
