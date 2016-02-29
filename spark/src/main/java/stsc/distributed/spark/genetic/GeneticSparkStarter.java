package stsc.distributed.spark.genetic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;
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
 * This is a default starter for genetic on spark search. TODO finish me!
 */
public final class GeneticSparkStarter {

	private final String sparkMaster = "local[4]";
	private final StrategySelector strategySelector = new StatisticsByCostSelector(150, generateDefaultCostFunction(), new MetricsDifferentComparator());

	private final int initialGenerationSize = 100;
	private final int minimalGenerationSize = 80;

	public GeneticSparkStarter() {
		Validate.isTrue(initialGenerationSize > minimalGenerationSize, "initialGenerationSize should be bigger then minimalGenerationSize");
	}

	public List<TradingStrategy> searchOnSpark() throws IOException, BadAlgorithmException {
		final SparkConf sparkConf = new SparkConf(). //
				setAppName(GridSparkStarter.class.getSimpleName()). //
				setMaster(sparkMaster);
		final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		final SimulatorSettingsGeneticListImpl geneticList = new GeneticRecordReader().generateSimulatorSettingsGeneticList(StockStorageMock.getStockStorage());

		List<TradingStrategyExternalizable> initialGeneration = calculateInitialGeneration(javaSparkContext, geneticList);

		javaSparkContext.close();

		final StockStorage stockStorage = StockStorageMock.getStockStorage();

		for (TradingStrategyExternalizable tsw : initialGeneration) {
			strategySelector.addStrategy(tsw.getTradingStrategy(stockStorage));
		}

		return strategySelector.getStrategies();
	}

	private List<TradingStrategyExternalizable> calculateInitialGeneration(final JavaSparkContext javaSparkContext,
			final SimulatorSettingsGeneticListImpl geneticList) throws BadAlgorithmException {

		final List<TradingStrategyExternalizable> result = new ArrayList<>();

		while (result.size() < minimalGenerationSize) {
			final List<SimulatorSettingsExternalizable> initialGeneration = createInitialGeneration(geneticList, initialGenerationSize - result.size());
			final JavaRDD<SimulatorSettingsExternalizable> simulatorSettings = javaSparkContext.parallelize(initialGeneration);
			final JavaRDD<TradingStrategyExternalizable> allTradingStrategies = simulatorSettings.map(new SimulatorMapper());
			final List<TradingStrategyExternalizable> initialPopulation = allTradingStrategies.collect();
			result.addAll(initialPopulation);
		}

		return result;
	}

	private List<SimulatorSettingsExternalizable> createInitialGeneration(final SimulatorSettingsGeneticListImpl geneticList, int amountToGenerate) throws BadAlgorithmException {
		final List<SimulatorSettingsExternalizable> result = new ArrayList<>();
		for (int i = 0; i < amountToGenerate; ++i) {
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
