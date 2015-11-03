package stsc.distributed.hadoop.grid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.hadoop.HadoopYahooStockStorage;
import stsc.distributed.hadoop.types.MetricsWritable;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;
import stsc.distributed.hadoop.types.TradingStrategyWritable;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.cost.comparator.MetricsDifferentComparator;
import stsc.general.statistic.cost.function.CostFunction;
import stsc.general.statistic.cost.function.CostWeightedProductFunction;
import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StatisticsByCostSelector;
import stsc.general.strategy.selector.StrategySelector;

/**
 * {@link SimulatorReducer} is an implementation for {@link Reducer} with next
 * generic parameters: <br/>
 * 1. Mapper output: <br/>
 * 1.a. {@link LongWritable} - currently is just a stubbed zero value, we don't
 * need complicated reducer for Grid example. <br/>
 * 1.b. {@link TradingStrategyWritable} - calculated trading strategy (settings
 * and metrics). <br/>
 * 2. Reducer output: <br/>
 * 2.a. {@link SimulatorSettingsWritable} as key - settings of the trading
 * strategy; <br/>
 * 2.b. {@link MetricsWritable} as value - calculated metrics. <br/>
 * 
 * Reduce process accumulate best strategies using hard coded example of
 * {@link CostFunction}.
 */
public final class SimulatorReducer extends Reducer<LongWritable, TradingStrategyWritable, SimulatorSettingsWritable, MetricsWritable> {

	private final HadoopYahooStockStorage hadoopYahooStockStorage;
	private StrategySelector strategySelector;

	public SimulatorReducer() throws IOException {
		this.hadoopYahooStockStorage = new HadoopYahooStockStorage();
		this.strategySelector = new StatisticsByCostSelector(150, generateDefaultCostFunction(), new MetricsDifferentComparator());
	}

	private CostFunction generateDefaultCostFunction() {
		final CostWeightedProductFunction cf = new CostWeightedProductFunction();
		cf.addParameter(MetricType.winProb, 2.5);
		cf.addParameter(MetricType.avLoss, -1.0);
		cf.addParameter(MetricType.avWin, 1.0);
		cf.addParameter(MetricType.startMonthAvGain, 1.2);
		cf.addParameter(MetricType.ddDurationAverage, -1.2);
		cf.addParameter(MetricType.ddValueAverage, -1.2);
		return cf;
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		hadoopYahooStockStorage.getStockStorage(context.getConfiguration());
	}

	@Override
	protected void reduce(LongWritable key, Iterable<TradingStrategyWritable> values, Context context) throws IOException, InterruptedException {
		final StockStorage stockStorage = hadoopYahooStockStorage.getStockStorage(context.getConfiguration());
		try {
			for (TradingStrategyWritable ts : values) {
				ts.getTradingStrategy(stockStorage);
				strategySelector.addStrategy(ts.getTradingStrategy(stockStorage));
			}
			for (TradingStrategy ts : strategySelector.getStrategies()) {
				final SimulatorSettingsWritable ssw = new SimulatorSettingsWritable(ts.getSettings());
				final MetricsWritable sw = new MetricsWritable(ts.getMetrics());
				context.write(ssw, sw);
			}
		} catch (BadAlgorithmException e) {
			throw new InterruptedException(e.getMessage());
		}
	}
}
