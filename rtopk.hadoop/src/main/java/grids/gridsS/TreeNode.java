package grids.gridsS;

import java.util.ArrayList;

import model.Cell_S;

public class TreeNode {

	private TreeNode parent;
	private ArrayList<TreeNode> children;
	private double nodeValue;
	private boolean leaf;
	private Cell_S cell;
	
	public TreeNode(TreeNode parent) {
		super();
		this.parent = parent;
		this.children = new ArrayList<TreeNode>();
	}

	/**
	 * @return the parent
	 */
	public TreeNode getParent() {
		return parent;
	}

	/**
	 * @param parent the parent to set
	 */
	public void setParent(TreeNode parent) {
		this.parent = parent;
	}

	/**
	 * @return the children
	 */
	public ArrayList<TreeNode> getChildren() {
		return children;
	}

	/**
	 * @param children the children to set
	 */
	public void setChildren(ArrayList<TreeNode> children) {
		this.children = children;
	}

	/**
	 * @return the nodeValue
	 */
	public double getNodeValue() {
		return nodeValue;
	}

	/**
	 * @param nodeValue the nodeValue to set
	 */
	public void setNodeValue(double nodeValue) {
		this.nodeValue = nodeValue;
	}

	/**
	 * @return the leaf
	 */
	public boolean isLeaf() {
		return leaf;
	}

	/**
	 * @param leaf the leaf to set
	 */
	public void setLeaf(boolean leaf) {
		this.leaf = leaf;
	}

	/**
	 * @return the cell
	 */
	public Cell_S getCell() {
		return cell;
	}

	/**
	 * @param cell the cell to set
	 */
	public void setCell(Cell_S cell) {
		this.cell = cell;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cell == null) ? 0 : cell.hashCode());
		result = prime * result + ((children == null) ? 0 : children.hashCode());
		result = prime * result + (leaf ? 1231 : 1237);
		long temp;
		temp = Double.doubleToLongBits(nodeValue);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((parent == null) ? 0 : parent.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TreeNode other = (TreeNode) obj;
		if (cell == null) {
			if (other.cell != null)
				return false;
		} else if (!cell.equals(other.cell))
			return false;
		if (children == null) {
			if (other.children != null)
				return false;
		} else if (!children.equals(other.children))
			return false;
		if (leaf != other.leaf)
			return false;
		if (Double.doubleToLongBits(nodeValue) != Double.doubleToLongBits(other.nodeValue))
			return false;
		if (parent == null) {
			if (other.parent != null)
				return false;
		} else if (!parent.equals(other.parent))
			return false;
		return true;
	}

	

	
}
