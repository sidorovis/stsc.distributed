package stsc.distributed.spark.grid;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import stsc.common.storage.StockStorage;
import stsc.distributed.common.types.SimulatorSettingsExternalizable;
import stsc.distributed.common.types.TradingStrategyExternalizable;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.cost.comparator.MetricsDifferentComparator;
import stsc.general.statistic.cost.function.CostFunction;
import stsc.general.statistic.cost.function.CostWeightedProductFunction;
import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StatisticsByCostSelector;
import stsc.general.strategy.selector.StrategySelector;
import stsc.storage.mocks.StockStorageMock;

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

	public List<TradingStrategy> searchOnSpark() throws Exception {

		final SparkConf sparkConf = new SparkConf(). //
				setAppName(GridSparkStarter.class.getSimpleName()). //
				setMaster("local");
		final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		final ArrayList<SimulatorSettingsExternalizable> simulatorSettingsList = new ArrayList<>();
		for (SimulatorSettingsExternalizable ss : new GridRecordReader().getGridList()) {
			simulatorSettingsList.add(ss);
		}
		final JavaRDD<SimulatorSettingsExternalizable> simulatorSettings = javaSparkContext.parallelize(simulatorSettingsList);
		final JavaRDD<TradingStrategyExternalizable> allTradingStrategies = simulatorSettings.map(new SimulatorMapper());

		List<TradingStrategyExternalizable> allmostRes = allTradingStrategies.collect();
		javaSparkContext.close();

		final StockStorage stockStorage = StockStorageMock.getStockStorage();

		final StrategySelector strategySelector = new StatisticsByCostSelector(150, generateDefaultCostFunction(), new MetricsDifferentComparator());
		for (TradingStrategyExternalizable tsw : allmostRes) {
			strategySelector.addStrategy(tsw.getTradingStrategy(stockStorage));
		}
		return strategySelector.getStrategies();
	}

}
