package stsc.distributed.spark.grid;

import org.junit.Assert;
import org.junit.Test;

public class GridSparkStarterTest {

	@Test
	public void testGridSparkStarter() throws Exception {
		final GridSparkStarter gridSparkStarter = new GridSparkStarter();
		Assert.assertEquals(8, gridSparkStarter.searchOnSpark().size());
	}

}
