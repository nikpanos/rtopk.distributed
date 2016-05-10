package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class MyKey implements WritableComparable {

	private IntWritable key;
	private ItemType type;

	public MyKey() {
		super();
		key = new IntWritable();
		type = ItemType.S;
	}

	public MyKey(IntWritable key, ItemType type) {
		super();
		this.key = key;
		this.type = type;
	}

	public IntWritable getKey() {
		return key;
	}

	public void setKey(IntWritable key) {
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
		key.readFields(input);
		int type = input.readInt();
		if (type == 0)
			this.type = ItemType.W;
		else if(type == 1)
			this.type = ItemType.S;
		else
			this.type = ItemType.W_InTopK;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		key.write(output);
		output.writeInt(type.getValue());
	}

	@Override
	public int compareTo(Object obj) {
		MyKey other = (MyKey) obj;

		if (this.key.compareTo(other.key) != 0)
			return other.key.compareTo(this.key);

		if (other.type == ItemType.S) {
			if (this.type == ItemType.W_InTopK) {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W_InTopK.
				return -1;
			} else if (this.type == ItemType.W) {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return 1;
			} else
				return 0;// are same
		} else if (other.type == ItemType.W) {
			if (this.type == ItemType.W_InTopK) {
				return -1;
			} else if (this.type == ItemType.W) {
				;// are same, do nothing
			} else {
				// Καθορίζει αν θα έρθουν πρώτα τα S ή τα W.
				return -1;
			}
		} else {
			if (this.type == ItemType.W_InTopK) {
				return 0;// are same
			} else if (this.type == ItemType.W) {
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
