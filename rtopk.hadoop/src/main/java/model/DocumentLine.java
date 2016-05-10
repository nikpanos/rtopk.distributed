package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentLine implements Writable {

	private Text text;
	private Text file;

	public DocumentLine() {
		super();
		text = new Text();
		file = new Text();
	}

	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	public Text getFile() {
		return file;
	}

	public void setFile(Text file) {
		this.file = file;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		text.readFields(input);
		file.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		text.write(output);
		file.write(output);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((file == null) ? 0 : file.hashCode());
		result = prime * result + ((text == null) ? 0 : text.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DocumentLine other = (DocumentLine) obj;
		if (file == null) {
			if (other.file != null)
				return false;
		} else if (!file.equals(other.file))
			return false;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		return true;
	}

}
