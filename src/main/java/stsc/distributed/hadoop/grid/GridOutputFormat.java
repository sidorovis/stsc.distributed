package stsc.distributed.hadoop.grid;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import stsc.distributed.hadoop.HadoopConfigurationHeader;
import stsc.distributed.hadoop.types.MetricsWritable;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;

/**
 * Implementation for {@link OutputFormat}. <br/>
 * <code>OutputFormat</code> describes the output-specification for a Map-Reduce
 * job.
 */
public final class GridOutputFormat extends OutputFormat<SimulatorSettingsWritable, MetricsWritable> {

	@Override
	public RecordWriter<SimulatorSettingsWritable, MetricsWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new GridRecordWriter(context.getConfiguration());
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new FileOutputCommitter(HadoopConfigurationHeader.getHdfsOutputFolderPath(context.getConfiguration()), context);
	}

}
