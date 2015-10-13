package stsc.distributed.hadoop.grid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import stsc.distributed.hadoop.HadoopConfigurationHeader;

/**
 * Implementation for {@link InputSplit}. <br/>
 * <code>InputSplit</code> represents the data to be processed by an individual
 * {@link Mapper}.
 */
public final class GridInputSplit extends InputSplit implements Writable {

	private int inputSplitSize;
	private String[] inputSplitLocations;

	public GridInputSplit(Configuration configuration) {
		this.inputSplitSize = HadoopConfigurationHeader.getInputSplitSize(configuration);
		this.inputSplitLocations = HadoopConfigurationHeader.getInputSplitLocations(configuration);
	}

	public GridInputSplit() {
		// this is required by Hadoop infrastructure
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return inputSplitSize;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return inputSplitLocations;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(inputSplitSize);
		out.writeInt(inputSplitLocations.length);
		for (String s : inputSplitLocations) {
			out.writeUTF(s);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		inputSplitSize = in.readInt();
		final int size = in.readInt();
		inputSplitLocations = new String[size];
		for (int i = 0; i < size; ++i) {
			inputSplitLocations[i] = in.readUTF();
		}
	}

}