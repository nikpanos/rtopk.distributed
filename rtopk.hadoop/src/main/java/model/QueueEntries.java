package model;

public class QueueEntries {
	private RTree.Node node;
	private float score;
	
	public QueueEntries(RTree.Node node, float score) {
		this.node = node;
		this.score = score;
	}

	public RTree.Node getNode() {
		return node;
	}

	public void setNode(RTree.Node node) {
		this.node = node;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}
	
	
}
