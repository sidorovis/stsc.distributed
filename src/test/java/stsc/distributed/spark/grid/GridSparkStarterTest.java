package stsc.distributed.spark.grid;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class GridSparkStarterTest {

	@Test
	public void test() throws IOException {
		final GridSparkStarter gridSparkStarter = new GridSparkStarter();
		Assert.assertEquals(8, gridSparkStarter.searchOnSpark().size());
	}

}
