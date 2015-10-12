package stsc.distributed.spark.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.cost.comparator.MetricsDifferentComparator;
import stsc.general.statistic.cost.function.CostFunction;
import stsc.general.statistic.cost.function.CostWeightedProductFunction;
import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StatisticsByCostSelector;
import stsc.general.strategy.selector.StrategySelector;

/**
 * This is a project that start distributed grid
 */
public final class GridSparkStarter {

	private CostFunction generateDefaultCostFunction() {
		final CostWeightedProductFunction cf = new CostWeightedProductFunction();
		cf.addParameter(MetricType.winProb, 2.5);
		cf.addParameter(MetricType.avLoss, -1.0);
		cf.addParameter(MetricType.avWin, 1.0);
		cf.addParameter(MetricType.startMonthAvGain, 1.2);
		cf.addParameter(MetricType.ddDurationAvGain, -1.2);
		cf.addParameter(MetricType.ddValueAvGain, -1.2);
		return cf;
	}

	public GridSparkStarter() {
	}

	public List<TradingStrategy> searchOnSpark() throws IOException {
		final SparkConf sparkConf = new SparkConf(). //
				setAppName(GridSparkStarter.class.getSimpleName()). //
				setMaster("local");
		final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		final ArrayList<SimulatorSettings> simulatorSettingsList = new ArrayList<>();
		for (SimulatorSettings ss : new GridRecordReader().getGridList()) {
			simulatorSettingsList.add(ss);
		}
		final JavaRDD<SimulatorSettings> simulatorSettings = javaSparkContext.parallelize(simulatorSettingsList);
		final JavaRDD<TradingStrategy> allTradingStrategies = simulatorSettings.map(new SimulatorMapper());

		final StrategySelector strategySelector = new StatisticsByCostSelector(150, generateDefaultCostFunction(), new MetricsDifferentComparator());
		allTradingStrategies.flatMap(new FlatMapFunction<TradingStrategy, StrategySelector>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<StrategySelector> call(TradingStrategy t) throws Exception {
				strategySelector.addStrategy(t);
				final ArrayList<StrategySelector> ss = new ArrayList<StrategySelector>();
				ss.add(strategySelector);
				return ss;
			}
		}).first();

		javaSparkContext.close();
		return strategySelector.getStrategies();
	}
}
