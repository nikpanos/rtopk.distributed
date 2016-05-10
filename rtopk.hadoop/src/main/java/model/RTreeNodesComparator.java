package model;

import java.util.Comparator;

public class RTreeNodesComparator implements Comparator<QueueEntries> {
	@Override
	public int compare(QueueEntries o1, QueueEntries o2) {
		if (o1.getScore() < o2.getScore()) {
			return -1;
		}
		else {//if (score1 > score2) {
			return 1;
		}
		//return 1;
	}

}
