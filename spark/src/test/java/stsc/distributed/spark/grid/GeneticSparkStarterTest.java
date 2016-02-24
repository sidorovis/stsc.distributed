package stsc.distributed.spark.grid;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import stsc.general.strategy.TradingStrategy;

public class GeneticSparkStarterTest {

	@Test
	public void testGeneticSparkStarter() throws IOException {
		final GeneticSparkStarter geneticSparkStarter = new GeneticSparkStarter();
		final List<TradingStrategy> searchOnSpark = geneticSparkStarter.searchOnSpark();
		Assert.assertTrue(searchOnSpark.isEmpty());
	}

}
