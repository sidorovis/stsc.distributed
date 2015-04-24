package stsc.distributed.hadoop.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

public class TradingStrategyWritable implements Writable {

	private TradingStrategy tradingStrategy;

	private SimulatorSettingsWritable ssw;
	private MetricsWritable sw;

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
		final SimulatorSettings settings = tradingStrategy.getSettings();
		final Metrics metrics = tradingStrategy.getMetrics();
		final SimulatorSettingsWritable ssw = new SimulatorSettingsWritable(settings);
		ssw.write(out);
		final MetricsWritable sw = new MetricsWritable(metrics);
		sw.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ssw = new SimulatorSettingsWritable();
		ssw.readFields(in);
		sw = new MetricsWritable();
		sw.readFields(in);
	}

	public TradingStrategy getTradingStrategy(final StockStorage stockStorage) throws BadAlgorithmException {
		return new TradingStrategy(ssw.getSimulatorSettings(stockStorage), sw.getMetrics());
	}
}
