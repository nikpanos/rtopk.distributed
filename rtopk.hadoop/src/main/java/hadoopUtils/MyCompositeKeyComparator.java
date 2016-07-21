package hadoopUtils;

import model.MyKey;

import org.apache.hadoop.io.WritableComparator;

public class MyCompositeKeyComparator extends WritableComparator {
    protected MyCompositeKeyComparator() {
        super(MyKey.class, true);
    }
    
    @Override 
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    	int i1 = readInt(b1, s1);
    	int i2 = readInt(b2, s2);
    	
    	int comp = i2 - i1;
        if(0 != comp)
            return comp;
        
        //comp = new ByteWritable.Comparator().compare(b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4);
        
        //int j1 = readByte(b1, s1+4);
        //int j2 = readInt(b2, s2+4);
        // = (j1 < j2) ? -1 : (j1 == j2) ? 0 : 1;
         
        return b1[s1 + 4] - b2[s2 + 4];
    }
    /*
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	MyKey k1 = (MyKey)w1;
    	MyKey k2 = (MyKey)w2;
         
    	int tmp = k2.getKey() - k1.getKey();
        
    	if (tmp == 0) {
    		return -(k1.getType().getValue() - k2.getType().getValue());
    	}
    	
    	return tmp;

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
    //}
}
