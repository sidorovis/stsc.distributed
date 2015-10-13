package stsc.distributed.spark.grid;

import org.apache.spark.api.java.function.Function;

import stsc.common.BadSignalException;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.common.types.SimulatorSettingsExternalizable;
import stsc.distributed.common.types.TradingStrategyExternalizable;
import stsc.general.simulator.Simulator;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;
import stsc.storage.mocks.StockStorageMock;

/**
 * Implementation for map method for spark.
 */
public final class SimulatorMapper implements Function<SimulatorSettingsExternalizable, TradingStrategyExternalizable> {

	private static final long serialVersionUID = -5095653617238110923L;

	@Override
	public TradingStrategyExternalizable call(final SimulatorSettingsExternalizable settings) throws Exception {
		final StockStorage stockStorage = StockStorageMock.getStockStorage();
		try {
			final SimulatorSettings simulatorSettings = settings.getSimulatorSettings(stockStorage);
			final Simulator simulator = new Simulator(simulatorSettings);
			final Metrics metrics = simulator.getMetrics();
			return new TradingStrategyExternalizable(new TradingStrategy(simulatorSettings, metrics));
		} catch (BadAlgorithmException | BadSignalException e) {
			throw new Exception(e.getMessage());
		}
	}

}
