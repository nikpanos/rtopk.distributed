package grids.gridsS;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import algorithms.Dominance;
import algorithms.FileParser;
import algorithms.Functions;
import model.Cell_S;
import model.MyItem;

public class GridS_TreeDominateAndAntidominateArea extends GridS {

	private TreeNode root;
	private int dimensionsNumber = 0;
	private int antidominateAreaCount = 0;
	
	public GridS_TreeDominateAndAntidominateArea(int dimensionsNumber) {
		super();
		this.dimensionsNumber = dimensionsNumber;
	}

	public GridS_TreeDominateAndAntidominateArea(URI gridSPath, float[] query) throws IOException {
		super();
		this.dimensionsNumber = query.length;
		FileParser.parseGridSFile(gridSPath, this);
		trimToSize(query);
	}
	
	public GridS_TreeDominateAndAntidominateArea(BufferedReader gridReader, float[] query) throws IOException {
		super();
		this.dimensionsNumber = query.length;
		FileParser.parseGridSFile(gridReader, this);
		trimToSize(query);
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
		int min = antidominateAreaCount;
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

	public void trimToSize(float[] query) {

		ArrayList<Cell_S> cells = getCells();
		ArrayList<Cell_S> copy = new ArrayList<Cell_S>(cells);

		for (Cell_S cell : copy) {
			boolean antidominate = true;
			boolean dominate = true;
			if (Dominance.dominateBoostQuery(query, cell.getUpperBound()) != -1) {
				antidominate = false;
			}
			if (Dominance.dominateBoostQuery(cell.getLowerBound(), query) != -1) {
				dominate = false;
			}
			if (antidominate) {
				antidominateAreaCount += cell.getCount();
				cells.remove(cell);
			}
			if (dominate) {
				cells.remove(cell);
			}
		}
		
		this.root = null;
		
		for (Cell_S cell_S : cells) {
			add(cell_S);
		}
	}
	
	/**
	 * @return the cells
	 */
	public ArrayList<Cell_S> getCells() {		
		ArrayList<Cell_S> cells = new ArrayList<>();
		int[] position = new int[dimensionsNumber];
		int currentDepth = 0;
		
		TreeNode current = root;
		w: while (current != null) {

			// for each children
			for (int i = position[currentDepth]; i < current.getChildren().size(); i++) {
				position[currentDepth] = i;
				if (current.getChildren().get(i).isLeaf()) {
					cells.add(current.getChildren().get(i).getCell());
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
		
		return cells;
	}
}
