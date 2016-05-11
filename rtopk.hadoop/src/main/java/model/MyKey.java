package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class MyKey implements WritableComparable {

	private int key;
	private ItemType type;

	public MyKey() {
		super();
		key = 0;
		type = ItemType.S;
	}

	public MyKey(int key, ItemType type) {
		super();
		this.key = key;
		this.type = type;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	/**
	 * @return the type
	 */
	public ItemType getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(ItemType type) {
		this.type = type;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		key = input.readInt();
		int type = input.readByte();
		if (type == 0)
			this.type = ItemType.W;
		else if(type == 1)
			this.type = ItemType.S;
		else
			this.type = ItemType.W_InTopK;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(key);
		output.writeByte(type.getValue());
	}

	@Override
	public int compareTo(Object obj) {
		MyKey other = (MyKey) obj;
		
		int tmp = other.key - this.key;
		
		if (tmp == 0) {
			return -(this.type.getValue() - other.type.getValue());
		}
		
		return tmp;

		/*if (this.key.compareTo(other.key) != 0)
			return other.key.compareTo(this.key);

		if (other.type == ItemType.S) {
			if (this.type == ItemType.W_InTopK) {
				// ��������� �� �� ������ ����� �� S � �� W_InTopK.
				return -1;
			} else if (this.type == ItemType.W) {
				// ��������� �� �� ������ ����� �� S � �� W.
				return 1;
			} else
				return 0;// are same
		} else if (other.type == ItemType.W) {
			if (this.type == ItemType.W_InTopK) {
				return -1;
			} else if (this.type == ItemType.W) {
				;// are same, do nothing
			} else {
				// ��������� �� �� ������ ����� �� S � �� W.
				return -1;
			}
		} else {
			if (this.type == ItemType.W_InTopK) {
				return 0;// are same
			} else if (this.type == ItemType.W) {
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
