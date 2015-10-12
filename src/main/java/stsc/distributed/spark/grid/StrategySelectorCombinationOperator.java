package stsc.distributed.spark.grid;

import org.apache.spark.api.java.function.Function2;

import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StrategySelector;

public final class StrategySelectorCombinationOperator implements Function2<StrategySelector, StrategySelector, StrategySelector> {

	private static final long serialVersionUID = 2856429760657011257L;

	@Override
	public StrategySelector call(StrategySelector v1, StrategySelector v2) throws Exception {
		if (v1 != v2) {
			for (TradingStrategy ts : v2.getStrategies()) {
				v1.addStrategy(ts);
			}
		}
		return v1;
	}

}
