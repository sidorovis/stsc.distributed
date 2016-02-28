package stsc.distributed.spark.genetic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.common.types.SimulatorSettingsExternalizable;
import stsc.distributed.common.types.TradingStrategyExternalizable;
import stsc.distributed.spark.grid.GridSparkStarter;
import stsc.distributed.spark.grid.SimulatorMapper;
import stsc.general.simulator.ExecutionImpl;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticListImpl;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.cost.comparator.MetricsDifferentComparator;
import stsc.general.statistic.cost.function.CostFunction;
import stsc.general.statistic.cost.function.CostWeightedProductFunction;
import stsc.general.strategy.TradingStrategy;
import stsc.general.strategy.selector.StatisticsByCostSelector;
import stsc.general.strategy.selector.StrategySelector;
import stsc.storage.mocks.StockStorageMock;

/**
 * This is a default starter for genetic on spark search.
 * TODO finish me!
 */
public final class GeneticSparkStarter {

	public GeneticSparkStarter() {
	}

	public List<TradingStrategy> searchOnSpark() throws IOException, BadAlgorithmException {

		final SparkConf sparkConf = new SparkConf(). //
				setAppName(GridSparkStarter.class.getSimpleName()). //
				setMaster("local[4]");
		final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		final SimulatorSettingsGeneticListImpl geneticList = new GeneticRecordReader().generateSimulatorSettingsGeneticList(StockStorageMock.getStockStorage());
		
		List<SimulatorSettingsExternalizable> initialGeneration = createInitialGeneration(geneticList);
		
		final JavaRDD<SimulatorSettingsExternalizable> simulatorSettings = javaSparkContext.parallelize(initialGeneration);
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

	private List<SimulatorSettingsExternalizable> createInitialGeneration(SimulatorSettingsGeneticListImpl geneticList) throws BadAlgorithmException {
		final List<SimulatorSettingsExternalizable> result = new ArrayList<>();
		for (int i = 0 ; i < 100 ; ++i) {
			final ExecutionImpl generateRandom = geneticList.generateRandom();
			result.add(new SimulatorSettingsExternalizable(generateRandom));
		}
		return result;
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

}
