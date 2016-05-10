package grids.gridsS;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;

import algorithms.FileParser;
import algorithms.Functions;
import model.Cell_S;
import model.MyItem;

public class GridS_Tree extends GridS {

	private TreeNode root;
	private int dimensionsNumber = 0;
	
	public GridS_Tree(int dimensionsNumber) {
		super();
		this.dimensionsNumber = dimensionsNumber;
	}

	public GridS_Tree(URI gridSPath, float[] query) throws IOException {
		super();
		this.dimensionsNumber = query.length;
		FileParser.parseGridSFile(gridSPath, this);
	}

	@Override
	public void add(Cell_S cell) {
		if (root == null) {
			root = new TreeNode(null);
			TreeNode current = root;
			for (int i = 0; i < cell.getLowerBound().length; i++) {
				TreeNode next = new TreeNode(current);
				next.setNodeValue(cell.getLowerBound()[i]);
				current.getChildren().add(next);
				if (i + 1 == cell.getLowerBound().length) {
					TreeNode leaf = new TreeNode(next);
					next.setLeaf(true);
					next.setCell(cell);
					next.getChildren().add(leaf);
				}
				current = next;
			}
		} else {
			TreeNode current = root;
			nodes: for (int i = 0; i < cell.getLowerBound().length; i++) {
				for (int j = 0; j < current.getChildren().size(); j++) {
					if (current.getChildren().get(j).getNodeValue() == cell.getLowerBound()[i]) {
						current = current.getChildren().get(j);
						continue nodes;
					}
				}
				TreeNode next = new TreeNode(current);
				next.setNodeValue(cell.getLowerBound()[i]);
				current.getChildren().add(next);
				Collections.sort(current.getChildren(), new ChildrenOrder());
				if (i + 1 == cell.getLowerBound().length) {
					TreeNode leaf = new TreeNode(next);
					next.setLeaf(true);
					next.setCell(cell);
					next.getChildren().add(leaf);
				}
				current = next;
			}
		}

	}

	@Override
	public int[] getCount(MyItem w, float[] query) {
		double queryScore = Functions.calculateScore(w, query);
		TreeNode current = root;
		int min = 0;
		int max = 0;
		int[] position = new int[dimensionsNumber];
		int currentDepth = 0;
		w: while (current != null) {

			// foreach children
			for (int i = position[currentDepth]; i < current.getChildren().size(); i++) {
				position[currentDepth] = i;
				if (current.getChildren().get(i).isLeaf()) {
					// Cell is under the Wi line
					if (queryScore > Functions.calculateScore(w,
							current.getChildren().get(i).getCell().getUpperBound()))
						min += current.getChildren().get(i).getCell().getCount();
					// Wi line intersect cell
					else if (queryScore > Functions.calculateScore(w,
							current.getChildren().get(i).getCell().getLowerBound())) {
						max += current.getChildren().get(i).getCell().getCount();
					} else {
						// do {
						 current = current.getParent();
						 position[currentDepth] = 0;
						 currentDepth--;
						// } while(position[currentDepth]==0);
						if (currentDepth >= 0)
							position[currentDepth]++;
						continue w;
					}
				} else {
					current = current.getChildren().get(i);
					currentDepth++;
					continue w;
				}
			}
			current = current.getParent();
			position[currentDepth] = 0;
			currentDepth--;
			if (currentDepth >= 0)
				position[currentDepth]++;
		}
		max += min;
		return new int[] { min, max };
	}

	@Override
	public int getAntidominateAreaCount(float[] query) {
		int antidominateAreaCount = 0;
		int[] position = new int[dimensionsNumber];
		int currentDepth = 0;
		
		TreeNode current = root;
		w: while (current != null) {

			// for each children
			for (int i = position[currentDepth]; i < current.getChildren().size(); i++) {
				position[currentDepth] = i;
				if (current.getChildren().get(i).isLeaf()) {
					boolean antidominate = true;
					for(int j=0;j<query.length;j++){
						if(current.getChildren().get(i).getCell().getUpperBound()[j]>query[j])
							antidominate = false;
					}
					if(antidominate)
						antidominateAreaCount += current.getChildren().get(i).getCell().getCount();
				} else {
					current = current.getChildren().get(i);
					currentDepth++;
					continue w;
				}
			}
			current = current.getParent();
			position[currentDepth] = 0;
			currentDepth--;
			if (currentDepth >= 0)
				position[currentDepth]++;
		}
		
		return antidominateAreaCount;
	}

	private class ChildrenOrder implements Comparator<TreeNode> {

		@Override
		public int compare(TreeNode node1, TreeNode node2) {
			if (node1.getNodeValue() < node2.getNodeValue())
				return -1;
			else if (node1.getNodeValue() > node2.getNodeValue())
				return 1;
			else
				return 0;
		}

	}
}
