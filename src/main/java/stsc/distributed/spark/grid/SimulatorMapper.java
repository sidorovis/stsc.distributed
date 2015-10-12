package stsc.distributed.spark.grid;

import org.apache.spark.api.java.function.Function;

import stsc.common.BadSignalException;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.Simulator;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

/**
 * Implementation for map method for spark.
 */
public final class SimulatorMapper implements Function<SimulatorSettings, TradingStrategy> {

	private static final long serialVersionUID = -5095653617238110923L;

	@Override
	public TradingStrategy call(final SimulatorSettings settings) throws Exception {
		// final StockStorage stockStorage = StockStorageMock.getStockStorage();
		try {
			final Simulator simulator = new Simulator(settings);
			final Metrics metrics = simulator.getMetrics();
			return new TradingStrategy(settings, metrics);
		} catch (BadAlgorithmException | BadSignalException e) {
			throw new Exception(e.getMessage());
		}
	}

}
