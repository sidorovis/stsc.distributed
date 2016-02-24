package stsc.distributed.spark.grid;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticListImpl;
import stsc.general.strategy.TradingStrategy;
import stsc.general.testhelper.TestGeneticSimulatorSettings;

/**
 * This is a default starter for genetic on spark search.
 */
public final class GeneticSparkStarter {

	public GeneticSparkStarter() {

	}

	public List<TradingStrategy> searchOnSpark() throws IOException {

		final SparkConf sparkConf = new SparkConf(). //
				setAppName(GridSparkStarter.class.getSimpleName()). //
				setMaster("local[4]");

		try (final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
			final SimulatorSettingsGeneticListImpl geneticList = TestGeneticSimulatorSettings.getBigGeneticList();
		}
		return Lists.newArrayList();
	}

}
