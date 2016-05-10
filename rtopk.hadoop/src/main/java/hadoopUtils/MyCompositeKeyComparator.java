package hadoopUtils;

import model.ItemType;
import model.MyKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompositeKeyComparator extends WritableComparator {
    protected MyCompositeKeyComparator() {
        super(MyKey.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	MyKey k1 = (MyKey)w1;
    	MyKey k2 = (MyKey)w2;
         
    	if (k1.getKey().compareTo(k2.getKey()) != 0)
			return k2.getKey().compareTo(k1.getKey());

		if (k2.getType() == ItemType.S) {
			if (k1.getType() == ItemType.W_InTopK) {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W_InTopK.
				return -1;
			} else if (k1.getType() == ItemType.W) {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return 1;
			} else
				return 0;// are same
		} else if (k2.getType() == ItemType.W) {
			if (k1.getType() == ItemType.W_InTopK) {
				return -1;
			} else if (k1.getType() == ItemType.W) {
				;// are same, do nothing
			} else {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return -1;
			}
		} else {
			if (k1.getType() == ItemType.W_InTopK) {
				return 0;// are same
			} else if (k1.getType() == ItemType.W) {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return 1;
			} else {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return 1;
			}
		}

		return 0;
    }
}
