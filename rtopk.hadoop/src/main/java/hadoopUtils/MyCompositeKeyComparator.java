package hadoopUtils;

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
         
    	int tmp = k2.getKey().compareTo(k1.getKey());
        
    	if (tmp != 0)
			return tmp;
    	
    	return -(k1.getType().getValue() - k2.getType().getValue());

		/*if (k2.getType() == ItemType.S) {
			if (k1.getType() == ItemType.W_InTopK) {
				// ��������� �� �� ������ ����� �� S � �� W_InTopK.
				return -1;
			} else if (k1.getType() == ItemType.W) {
				// ��������� �� �� ������ ����� �� S � �� W.
				return 1;
			} else
				return 0;// are same
		} else if (k2.getType() == ItemType.W) {
			if (k1.getType() == ItemType.W_InTopK) {
				return -1;
			} else if (k1.getType() == ItemType.W) {
				;// are same, do nothing
			} else {
				// ��������� �� �� ������ ����� �� S � �� W.
				return -1;
			}
		} else {
			if (k1.getType() == ItemType.W_InTopK) {
				return 0;// are same
			} else if (k1.getType() == ItemType.W) {
				// ��������� �� �� ������ ����� �� S � �� W.
				return 1;
			} else {
				// ��������� �� �� ������ ����� �� S � �� W.
				return 1;
			}
		}

		return 0;*/
    }
}
