package stsc.distributed.common.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.Execution;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

/**
 * This is implementation for {@link Externalizable} of {@link TradingStrategy}.
 */
public final class TradingStrategyExternalizable implements Externalizable {

	private TradingStrategy tradingStrategy;

	private SimulatorSettingsExternalizable simulatorSettingsWritable;
	private MetricsExternalizable metricsWritable;

	public TradingStrategyExternalizable(TradingStrategy ts) {
		this.tradingStrategy = ts;
	}

	/**
	 * For reading
	 */
	public TradingStrategyExternalizable() {
	}

	public TradingStrategy getTradingStrategy(final StockStorage stockStorage) throws BadAlgorithmException {
		return new TradingStrategy(simulatorSettingsWritable.getSimulatorSettings(stockStorage), metricsWritable.getMetrics());
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		final Execution settings = tradingStrategy.getSettings();
		final Metrics metrics = tradingStrategy.getMetrics();
		final SimulatorSettingsExternalizable ssw = new SimulatorSettingsExternalizable(settings);
		ssw.writeExternal(out);
		final MetricsExternalizable sw = new MetricsExternalizable(metrics);
		sw.writeExternal(out);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		simulatorSettingsWritable = new SimulatorSettingsExternalizable();
		simulatorSettingsWritable.readExternal(in);
		metricsWritable = new MetricsExternalizable();
		metricsWritable.readExternal(in);
	}
}
