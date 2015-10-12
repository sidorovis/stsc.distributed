package stsc.distributed.hadoop.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;
import stsc.distributed.hadoop.HadoopConfigurationHeader;
import stsc.distributed.hadoop.HadoopYahooStockStorage;
import stsc.distributed.hadoop.types.MetricsWritable;
import stsc.distributed.hadoop.types.TradingStrategyWritable;
import stsc.general.strategy.TradingStrategy;

/**
 * GridRecordWriter is an implementation for {@link RecordWriter}. <br/>
 * Pair for RecordWriter is : {@link SimulatorSettingsWritable} to
 * {@link MetricsWritable}. <br/>
 * This RecordWriter accumulate all trading strategies from mappers and save all
 * of them to the HDFS (using
 * {@link HadoopConfigurationHeader#getHdfsOutputFilePath(Configuration)}).
 */
public final class GridRecordWriter extends RecordWriter<SimulatorSettingsWritable, MetricsWritable> {

	private final StockStorage stockStorage;
	private final List<TradingStrategy> tradingStrategies = Collections.synchronizedList(new ArrayList<TradingStrategy>());

	public GridRecordWriter(Configuration configuration) throws IOException {
		this.stockStorage = new HadoopYahooStockStorage().getStockStorage(configuration);
	}

	@Override
	public void write(final SimulatorSettingsWritable key, final MetricsWritable value) throws IOException, InterruptedException {
		try {
			tradingStrategies.add(new TradingStrategy(key.getSimulatorSettings(stockStorage), value.getMetrics()));
		} catch (BadAlgorithmException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
		final Path hdfsOutputPath = HadoopConfigurationHeader.getHdfsOutputFilePath(context.getConfiguration());

		final FileSystem fs = FileSystem.get(context.getConfiguration());
		if (fs.isDirectory(hdfsOutputPath)) {
			fs.delete(hdfsOutputPath, true);
		}
		if (fs.isFile(hdfsOutputPath)) {
			fs.delete(hdfsOutputPath, true);
		}
		final FSDataOutputStream fileOut = fs.create(hdfsOutputPath, true); // overwrite
		fileOut.writeInt(tradingStrategies.size());
		for (TradingStrategy ts : tradingStrategies) {
			final TradingStrategyWritable tsw = new TradingStrategyWritable(ts);
			tsw.write(fileOut);
		}
		fileOut.close();
	}

}
