package stsc.distributed.spark.grid;

import org.apache.spark.api.java.function.Function2;

import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StrategySelector;

/**
 * Implementation for reducer
 */
public class SimulatorReducer implements Function2<StrategySelector, TradingStrategy, StrategySelector> {

	private static final long serialVersionUID = -2126252315496261975L;

	@Override
	public StrategySelector call(StrategySelector ss, TradingStrategy ts) throws Exception {
		ss.addStrategy(ts);
		return ss;
	}

}
