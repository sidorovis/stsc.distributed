package stsc.distributed.hadoop.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import stsc.distributed.hadoop.HadoopConfigurationHeader;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;

/**
 * Implementation for <code>InputFormat</code> describes the input-specification
 * for a Map-Reduce job. <br/>
 * For tests purposes configure List of {@link InputSplit}'s.
 */
public final class GridInputFormat extends InputFormat<LongWritable, SimulatorSettingsWritable> {

	public GridInputFormat() {
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		final int size = HadoopConfigurationHeader.getInputSplitSize(context.getConfiguration());
		final List<InputSplit> splits = new ArrayList<InputSplit>(size);
		for (int i = 0; i < size; ++i) {
			splits.add(new GridInputSplit(context.getConfiguration()));
		}
		return splits;
	}

	@Override
	public RecordReader<LongWritable, SimulatorSettingsWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new GridRecordReader(context.getConfiguration());
	}

}