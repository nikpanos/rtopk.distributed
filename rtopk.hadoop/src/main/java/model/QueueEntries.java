package model;

public class QueueEntries {
	private RTree.Node node;
	private double score;
	
	public QueueEntries(RTree.Node node, double score) {
		this.node = node;
		this.score = score;
	}

	public RTree.Node getNode() {
		return node;
	}

	public void setNode(RTree.Node node) {
		this.node = node;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	
}
