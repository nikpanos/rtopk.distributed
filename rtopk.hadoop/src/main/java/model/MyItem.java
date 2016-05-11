package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyItem implements Writable {

	private long id;
	public float[] values;
	private ItemType itemType = ItemType.S;

	public MyItem() {
		super();
	}

	public MyItem(long id, ItemType itemType) {
		super();
		this.id = id;
		this.itemType = itemType;
	}

	public MyItem(long id, float[] values) {
		super();
		this.id = id;
		this.values = values;
	}

	public MyItem(long id, float[] values, ItemType itemType) {
		super();
		this.id = id;
		this.values = values;
		this.itemType = itemType;
	}

	/*
	 * To be used by the RTree component
	 */
	public MyItem(float[] values) {
		super();
		this.values = values;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	/*
	 * For use in the RTree
	 */
	public float[] getFields() {
		return values;
	}

	public float[] getValues() {
		return values;
	}

	public void setValues(float[] values) {
		this.values = values;
	}

	public ItemType getItemType() {
		return itemType;
	}

	public void setItemType(ItemType itemType) {
		this.itemType = itemType;
	}

	public Text valuesToText() {
		String text = "[ ";
		for (int i = 0; i < values.length; i++) {
			text += values[i];
			if (i + 1 < values.length)
				text += " , ";
		}
		text += " ]";
		return new Text(text);
	}
	/*
	public String toString() {
		StringBuilder builder;
		switch (itemType) {
		case S:
			builder = new StringBuilder("S\t");
			break;
		case W:
			builder = new StringBuilder("S\t");
			break;
		default:
			builder = new StringBuilder("W_In_topk\t");
			break;
		}
		
		builder.append(id);
		
		for (int i = 0; i < values.length; i++) {
			builder.append(values[i] + "\t");
		}
		
		return builder.toString();
	}*/

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((itemType == null) ? 0 : itemType.hashCode());
		result = prime * result + Arrays.hashCode(values);
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
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
		MyItem other = (MyItem) obj;
		if (id != other.id)
			return false;
		if (itemType != other.itemType)
			return false;
		if (!Arrays.equals(values, other.values))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		id = input.readLong();
		int type = input.readByte();
		if (type == 0)
			itemType = ItemType.W;
		else if (type == 1)
			itemType = ItemType.S;
		else
			itemType = ItemType.W_InTopK;
		
		values = new float[input.readByte()];
		for (int i = 0; i < values.length; i++) {
			values[i] = input.readFloat();
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(id);
		output.writeByte(itemType.getValue());
		output.writeByte(values.length);
		for (int i = 0; i < values.length; i++) {
			output.writeFloat(values[i]);
		}
	}

}
