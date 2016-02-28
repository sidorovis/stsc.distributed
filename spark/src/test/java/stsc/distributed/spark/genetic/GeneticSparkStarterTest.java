package stsc.distributed.spark.genetic;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.distributed.spark.genetic.GeneticSparkStarter;
import stsc.general.strategy.TradingStrategy;

public class GeneticSparkStarterTest {

	@Test
	public void testGeneticSparkStarter() throws IOException, BadAlgorithmException {
		final GeneticSparkStarter geneticSparkStarter = new GeneticSparkStarter();
		final List<TradingStrategy> searchOnSpark = geneticSparkStarter.searchOnSpark();
		Assert.assertFalse(searchOnSpark.isEmpty());
		Assert.assertEquals(100, searchOnSpark.size());
	}

}
