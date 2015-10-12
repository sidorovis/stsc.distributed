package stsc.distributed.common.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

/**
 * This is implementation for {@link Writable} of {@link TradingStrategy}.
 */
public final class TradingStrategyWritable implements Externalizable {

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

	public TradingStrategy getTradingStrategy(final StockStorage stockStorage) throws BadAlgorithmException {
		return new TradingStrategy(simulatorSettingsWritable.getSimulatorSettings(stockStorage), metricsWritable.getMetrics());
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		final SimulatorSettings settings = tradingStrategy.getSettings();
		final Metrics metrics = tradingStrategy.getMetrics();
		final SimulatorSettingsWritable ssw = new SimulatorSettingsWritable(settings);
		ssw.writeExternal(out);
		final MetricsWritable sw = new MetricsWritable(metrics);
		sw.writeExternal(out);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		simulatorSettingsWritable = new SimulatorSettingsWritable();
		simulatorSettingsWritable.readExternal(in);
		metricsWritable = new MetricsWritable();
		metricsWritable.readExternal(in);
	}
}
