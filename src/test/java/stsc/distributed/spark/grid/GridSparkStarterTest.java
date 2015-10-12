package stsc.distributed.spark.grid;

import org.junit.Assert;
import org.junit.Test;

public class GridSparkStarterTest {

	@Test
	public void test() {
		final GridSparkStarter gridSparkStarter = new GridSparkStarter();
		Assert.assertNotNull(gridSparkStarter);
	}

}
