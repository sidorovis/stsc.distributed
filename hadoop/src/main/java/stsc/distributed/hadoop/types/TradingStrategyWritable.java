package stsc.distributed.hadoop.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.Execution;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

/**
 * This is implementation for {@link Writable} of {@link TradingStrategy}.
 */
public final class TradingStrategyWritable implements Writable {

	private TradingStrategy tradingStrategy;

	private SimulatorSettingsWritable simulatorSettingsWritable;
	private MetricsWritable metricsWritable;

	public TradingStrategyWritable(TradingStrategy ts) {
		this.tradingStrategy = ts;
	}

	/**
	 * For reading
	 */
	public TradingStrategyWritable() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		final Execution settings = tradingStrategy.getSettings();
		final Metrics metrics = tradingStrategy.getMetrics();
		final SimulatorSettingsWritable ssw = new SimulatorSettingsWritable(settings);
		ssw.write(out);
		final MetricsWritable sw = new MetricsWritable(metrics);
		sw.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		simulatorSettingsWritable = new SimulatorSettingsWritable();
		simulatorSettingsWritable.readFields(in);
		metricsWritable = new MetricsWritable();
		metricsWritable.readFields(in);
	}

	public TradingStrategy getTradingStrategy(final StockStorage stockStorage) throws BadAlgorithmException {
		return new TradingStrategy(simulatorSettingsWritable.getSimulatorSettings(stockStorage), metricsWritable.getMetrics());
	}
}
